package search

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/umputun/remark42/backend/app/store"
)

func createTestService(t *testing.T) (searcher Searcher, teardown func()) {
	tmp := os.TempDir()
	idxPath := tmp + "/search-remark42"

	_ = os.RemoveAll(idxPath)

	searcher, err := NewSearcher("bleve",
		SearcherParams{
			IndexPath: idxPath,
			Analyzer:  "standard",
		})
	require.NoError(t, err)

	teardown = func() {
		require.NoError(t, searcher.Close())
		_ = os.RemoveAll(idxPath)
	}
	return searcher, teardown
}

func TestSearch_SiteMux(t *testing.T) {
	searcher, teardown := createTestService(t)
	defer teardown()

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
	{
		res, err := searcher.Search(&Request{SiteID: "test-site", Query: "123", Limit: 3})
		require.NoError(t, err)
		require.Len(t, res.Documents, 1)
		assert.Equal(t, "123456", res.Documents[0].ID)

		res, err = searcher.Search(&Request{SiteID: "test-site", Query: "345", Limit: 3})
		require.NoError(t, err)
		require.Len(t, res.Documents, 0)
	}
	{
		res, err := searcher.Search(&Request{SiteID: "test-site2", Query: "345", SortBy: "-timestamp", Limit: 3})
		require.NoError(t, err)
		require.Len(t, res.Documents, 2)
		assert.Equal(t, "123457", res.Documents[0].ID)
		assert.Equal(t, "123456", res.Documents[1].ID)

		res, err = searcher.Search(&Request{SiteID: "test-site2", Query: "123", Limit: 3})
		require.NoError(t, err)
		require.Len(t, res.Documents, 0)
	}
	{
		res, err := searcher.Search(&Request{SiteID: "test-site3", Query: "345", Limit: 3})
		require.NoError(t, err)
		require.Len(t, res.Documents, 0)
	}
}
