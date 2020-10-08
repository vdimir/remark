package internal

import (
	"context"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/umputun/remark42/backend/app/store"
	"github.com/umputun/remark42/backend/app/store/search/types"
)

type MockTransport struct {
	Response    *http.Response
	RoundTripFn func(req *http.Request) (*http.Response, error)
}

func (t *MockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return t.RoundTripFn(req)
}

func mockElasticClient(t *testing.T) *elasticsearch.Client {
	t.Helper()

	mocktrans := MockTransport{
		Response: &http.Response{
			StatusCode: http.StatusOK,
			Body:       ioutil.NopCloser(strings.NewReader(`{}`)),
		},
	}
	mocktrans.RoundTripFn = func(req *http.Request) (*http.Response, error) { return mocktrans.Response, nil }

	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Transport: &mocktrans,
	})
	require.NoError(t, err)

	return client
}

func TestElasticSimple(t *testing.T) {
	client := mockElasticClient(t)

	esearcher, err := newElasticServiceWithClient(types.SearcherParams{
		Type:  "elastic",
		Sites: []string{"test-site"},
	}, client)
	assert.NoError(t, err)

	err = esearcher.Init(context.TODO(), nil)
	assert.NoError(t, err)

	assert.True(t, esearcher.Ready())

	err = esearcher.IndexDocument(&store.Comment{
		ID:        "123457",
		Locator:   store.Locator{SiteID: "test-site", URL: "http://example.com/post1"},
		Text:      "foobar 345",
		User:      store.User{ID: "u1", Name: "user1"},
		Timestamp: time.Date(2017, 12, 20, 15, 20, 28, 0, time.Local),
	})
	assert.NoError(t, err)

	res, err := esearcher.Search(&types.Request{SiteID: "test-site", Query: "123", Limit: 3})
	assert.NoError(t, err)
	assert.Len(t, res.Documents, 0)

	assert.NoError(t, esearcher.Delete("test-site", "1111"))
	assert.False(t, esearcher.Help() == "")
	assert.NoError(t, esearcher.Close())
}
