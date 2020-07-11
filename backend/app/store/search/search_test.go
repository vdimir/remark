package search

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	log "github.com/go-pkgz/lgr"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/umputun/remark42/backend/app/store"
	"github.com/umputun/remark42/backend/app/store/engine"
	bolt "go.etcd.io/bbolt"
)

func createTestService(t *testing.T, sites []string) (searcher Service, teardown func()) {
	tmp := os.TempDir()
	idxPath := tmp + "/search-remark42"

	_ = os.RemoveAll(idxPath)

	searcher, err := NewSearcher(SearcherParams{
		Type:      "bleve",
		IndexPath: idxPath,
		Analyzer:  "standard",
		Sites:     sites,
	})

	require.NoError(t, err)

	teardown = func() {

		err := searcher.Close()
		require.NoError(t, err)
		_ = os.RemoveAll(idxPath)
	}
	return searcher, teardown
}

func TestSearch_SiteMux(t *testing.T) {
	searcher, teardown := createTestService(t, []string{"test-site", "test-site2", "test-site3"})
	defer teardown()
	err := searcher.Init(context.Background(), nil)
	assert.NoError(t, err)

	searcher.IndexDocument("123456", &store.Comment{
		ID:        "123456",
		Locator:   store.Locator{SiteID: "test-site", URL: "http://example.com/post1"},
		Text:      "text 123",
		User:      store.User{ID: "u1", Name: "user1"},
		Timestamp: time.Date(2017, 12, 20, 15, 18, 24, 0, time.Local),
	})

	searcher.IndexDocument("123456", &store.Comment{
		ID:        "123456",
		Locator:   store.Locator{SiteID: "test-site2", URL: "http://example.com/post1"},
		Text:      "text 345",
		User:      store.User{ID: "u1", Name: "user1"},
		Timestamp: time.Date(2017, 12, 20, 15, 20, 24, 0, time.Local),
	})

	searcher.IndexDocument("123457", &store.Comment{
		ID:        "123457",
		Locator:   store.Locator{SiteID: "test-site2", URL: "http://example.com/post1"},
		Text:      "foobar 345",
		User:      store.User{ID: "u1", Name: "user1"},
		Timestamp: time.Date(2017, 12, 20, 15, 20, 28, 0, time.Local),
	})

	_ = searcher.Flush("test-site")
	_ = searcher.Flush("test-site2")
	{
		res, err := searcher.Search(&Request{SiteID: "test-site", Query: "123", Limit: 3})
		require.NoError(t, err)
		require.Len(t, res.Documents, 1)
		assert.Equal(t, "123456", res.Documents[0].ID)

		require.Len(t, res.Documents[0].Matches, 1)
		assert.Equal(t, res.Documents[0].Matches[0], TokenMatch{5, 8})

		res, err = searcher.Search(&Request{SiteID: "test-site", Query: "345", Limit: 3})
		require.NoError(t, err)
		require.Len(t, res.Documents, 0)
	}
	{
		res, err := searcher.Search(&Request{SiteID: "test-site2", Query: "345", SortBy: "-timestamp", Limit: 3})
		require.NoError(t, err)
		require.Len(t, res.Documents, 2)
		assert.Equal(t, "123457", res.Documents[0].ID)
		require.Len(t, res.Documents[0].Matches, 1)
		assert.Equal(t, res.Documents[0].Matches[0], TokenMatch{7, 10})

		assert.Equal(t, "123456", res.Documents[1].ID)
		require.Len(t, res.Documents[1].Matches, 1)
		assert.Equal(t, res.Documents[1].Matches[0], TokenMatch{5, 8})

		res, err = searcher.Search(&Request{SiteID: "test-site2", Query: "123", Limit: 3})
		require.NoError(t, err)
		assert.Len(t, res.Documents, 0)
	}
	{
		res, err := searcher.Search(&Request{SiteID: "test-site3", Query: "345", Limit: 3})
		require.NoError(t, err)
		assert.Len(t, res.Documents, 0)
	}
}

func TestSearch_Paginate(t *testing.T) {
	searcher, teardown := createTestService(t, []string{"test-site"})
	defer teardown()
	err := searcher.Init(context.Background(), nil)
	assert.NoError(t, err)

	t0 := time.Date(2017, 12, 20, 15, 18, 24, 0, time.Local)
	for shift := 0; shift < 4; shift++ {
		cid := fmt.Sprintf("comment%d", shift)
		searcher.IndexDocument(cid, &store.Comment{
			ID:        cid,
			Locator:   store.Locator{SiteID: "test-site", URL: fmt.Sprintf("http://example.com/post%d", shift%2)},
			Text:      "text 123",
			User:      store.User{ID: "u1", Name: "user1"},
			Timestamp: t0.Add(time.Duration(shift) * time.Minute),
		})
	}

	_ = searcher.Flush("test-site")
	{
		res, err := searcher.Search(&Request{SiteID: "test-site", Query: "123", Limit: 1, From: 0})
		require.NoError(t, err)
		require.Len(t, res.Documents, 1)
		assert.Equal(t, "comment0", res.Documents[0].ID)
	}
	{
		res, err := searcher.Search(&Request{SiteID: "test-site", Query: "123", Limit: 1, From: 1})
		require.NoError(t, err)
		require.Len(t, res.Documents, 1)
		assert.Equal(t, "comment1", res.Documents[0].ID)
	}
	{
		res, err := searcher.Search(&Request{SiteID: "test-site", Query: "123", Limit: 1, From: 3})
		require.NoError(t, err)
		require.Len(t, res.Documents, 1)
		assert.Equal(t, "comment3", res.Documents[0].ID)
	}
	{
		res, err := searcher.Search(&Request{SiteID: "test-site", Query: "123", Limit: 2, From: 1, SortBy: "-timestamp"})
		require.NoError(t, err)
		require.Len(t, res.Documents, 2)
		assert.Equal(t, []string{"comment2", "comment1"}, []string{res.Documents[0].ID, res.Documents[1].ID})
	}
}

func createDB(t *testing.T, commentsPerSite int, sites []string) (e engine.Interface, teardown func()) {
	testDB := os.TempDir() + "/remark-db"
	_ = os.RemoveAll(testDB)
	err := os.MkdirAll(testDB, 0700)
	require.NoError(t, err)
	bsites := []engine.BoltSite{}
	for _, s := range sites {
		bsites = append(bsites, engine.BoltSite{FileName: testDB + "/" + s, SiteID: s})
	}
	b, err := engine.NewBoltDB(bolt.Options{}, bsites...)
	require.NoError(t, err)
	teardown = func() {
		require.NoError(t, b.Close())
		_ = os.RemoveAll(testDB)
	}

	rng := rand.New(rand.NewSource(42))

	t0 := time.Date(2017, 12, 20, 15, 18, 24, 0, time.Local)
	for _, siteID := range sites {
		for shift := 0; shift < commentsPerSite; shift++ {
			cid := fmt.Sprintf("comment%d", shift)
			uid := rng.Intn(15)
			comment := store.Comment{
				ID:        cid,
				Locator:   store.Locator{SiteID: siteID, URL: fmt.Sprintf("http://example.com/post%d", rng.Intn(10))},
				Text:      fmt.Sprintf("%d text %d", rng.Int63(), rng.Int63()),
				User:      store.User{ID: fmt.Sprintf("u%d", uid), Name: fmt.Sprintf("user %d", uid)},
				Timestamp: t0.Add(time.Duration(shift) * time.Hour),
			}
			ccid, err := b.Create(comment)
			require.NoError(t, err)
			require.Equal(t, cid, ccid)
		}
	}

	return b, teardown
}

func TestSearch_IndexStartup(t *testing.T) {
	sites := []string{"test-site", "remark", "test-site42"}

	searcher, teardown := createTestService(t, sites)
	defer teardown()

	b, teardown := createDB(t, 42, sites)
	defer teardown()

	err := searcher.Init(context.Background(), b)
	assert.NoError(t, err)

	for _, siteID := range sites {
		_ = searcher.Flush(siteID)
	}

	for _, siteID := range sites {
		serp, err := searcher.Search(&Request{
			SiteID: siteID,
			Query:  "text",
			Limit:  19,
		})
		assert.NoError(t, err)
		assert.Len(t, serp.Documents, 19)
	}
}

func TestSearch_Delete(t *testing.T) {
	searcher, teardown := createTestService(t, []string{"test-site"})
	defer teardown()
	err := searcher.Init(context.Background(), nil)
	assert.NoError(t, err)

	timestamp := time.Date(2017, 12, 20, 15, 18, 24, 0, time.Local)

	err = searcher.IndexDocument("comment1", &store.Comment{
		ID:        "comment1",
		Locator:   store.Locator{SiteID: "test-site", URL: "http://example.com/post"},
		Text:      "text 123",
		User:      store.User{ID: "u1", Name: "user1"},
		Timestamp: timestamp,
	})
	require.NoError(t, err)

	err = searcher.IndexDocument("comment2", &store.Comment{
		ID:        "comment2",
		Locator:   store.Locator{SiteID: "test-site", URL: "http://example.com/post"},
		Text:      "text 345",
		User:      store.User{ID: "u1", Name: "user1"},
		Timestamp: timestamp.Add(time.Hour),
	})
	require.NoError(t, err)

	_ = searcher.Flush("test-site")

	{
		res, searchErr := searcher.Search(&Request{SiteID: "test-site", Query: "text", SortBy: "+timestamp", Limit: 10})
		require.NoError(t, searchErr)
		require.Len(t, res.Documents, 2)
		assert.Equal(t, "comment1", res.Documents[0].ID)
		assert.Equal(t, "comment2", res.Documents[1].ID)
	}
	err = searcher.Delete("test-site", "comment1")
	require.NoError(t, err)

	{
		res, searchErr := searcher.Search(&Request{SiteID: "test-site", Query: "text", SortBy: "+timestamp", Limit: 10})
		require.NoError(t, searchErr)
		require.Len(t, res.Documents, 1)
		assert.Equal(t, "comment2", res.Documents[0].ID)
	}
}

func TestSearch_OtherFields(t *testing.T) {
	searcher, teardown := createTestService(t, []string{"test-site", "test-site2", "test-site3"})
	defer teardown()
	err := searcher.Init(context.Background(), nil)
	assert.NoError(t, err)

	searcher.IndexDocument("123456", &store.Comment{
		ID:        "123456",
		Locator:   store.Locator{SiteID: "test-site", URL: "http://example.com/post1"},
		Text:      "text 123",
		User:      store.User{ID: "u1", Name: "user foo"},
		Timestamp: time.Date(2017, 12, 18, 15, 18, 24, 0, time.Local),
	})
	searcher.IndexDocument("123457", &store.Comment{
		ID:        "123457",
		Locator:   store.Locator{SiteID: "test-site", URL: "http://example.com/post1"},
		Text:      "text 345",
		User:      store.User{ID: "u2", Name: "User Bar"},
		Timestamp: time.Date(2017, 12, 21, 15, 20, 24, 0, time.Local),
	})
	searcher.IndexDocument("123458", &store.Comment{
		ID:        "123458",
		Locator:   store.Locator{SiteID: "test-site", URL: "http://example.com/post1"},
		Text:      "foobar text",
		User:      store.User{ID: "u2", Name: "User Bar"},
		Timestamp: time.Date(2017, 12, 25, 16, 20, 28, 0, time.Local),
	})

	_ = searcher.Flush("test-site")

	// username
	{
		res, err := searcher.Search(&Request{SiteID: "test-site", Query: "text +username:\"user bar\"", Limit: 20})
		require.NoError(t, err)
		require.Len(t, res.Documents, 2)
	}
	{
		res, err := searcher.Search(&Request{SiteID: "test-site", Query: "text +username:\"user foo\"", Limit: 20})
		require.NoError(t, err)
		require.Len(t, res.Documents, 1)
	}
	{
		// order matters in username field, match only whole token
		res, err := searcher.Search(&Request{SiteID: "test-site", Query: "text +username:\"foo user\"", Limit: 20})
		require.NoError(t, err)
		require.Len(t, res.Documents, 0)
	}

	// time range
	{
		res, err := searcher.Search(&Request{SiteID: "test-site", Query: "text +timestamp:>\"2017-12-20\"", Limit: 20})
		require.NoError(t, err)
		require.Len(t, res.Documents, 2)
	}
	{
		res, err := searcher.Search(&Request{SiteID: "test-site", Query: "text +timestamp:<\"2017-12-20\"", Limit: 20})
		require.NoError(t, err)
		require.Len(t, res.Documents, 1)
	}
}

func TestMain(m *testing.M) {
	log.Setup(log.Debug, log.CallerFile, log.CallerFunc, log.Msec, log.LevelBraces)
	os.Exit(m.Run())
}
