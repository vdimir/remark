// Package search provides full-text search functionality for site
package search

import (
	"encoding/hex"
	"hash/fnv"
	"path"

	"github.com/pkg/errors"
	"github.com/umputun/remark42/backend/app/store"
)

// Request is the input for Search
type Request struct {
	SiteID string
	Query  string
	SortBy string
	From   int
	Limit  int
}

// TokenMatch describes match position
type TokenMatch struct {
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
}

// ResultDoc search result document
type ResultDoc struct {
	PostURL string       `json:"url"`
	ID      string       `json:"id"`
	Matches []TokenMatch `json:"matches"`
}

// ResultPage returned from search
type ResultPage struct {
	Total     uint64      `json:"total"`
	Documents []ResultDoc `json:"documetns"`
}

// Searcher provides common interface for search engines
type Searcher interface {
	IndexDocument(commentID string, comment *store.Comment) error
	Search(req *Request) (*ResultPage, error)
	Delete(siteID, commentID string) error
	Close() error
}

func encodeSiteID(siteID string) string {
	h := fnv.New32().Sum([]byte(siteID))
	return hex.EncodeToString(h)
}

var engineMap map[string]searcherFactory = map[string]searcherFactory{
	"bleve": func(siteID string, p SearcherParams) (Searcher, error) {
		fpath := path.Join(p.IndexPath, encodeSiteID(siteID))
		return newBleveService(fpath, p.Analyzer)
	},
}

// NewSearcher creates new searcher with specified type and parameters
func NewSearcher(engine string, params SearcherParams) (Searcher, error) {
	f, has := engineMap[engine]
	if !has {
		available := []string{}
		for k := range engineMap {
			available = append(available, k)
		}
		return nil, errors.Errorf("no search engine %q, available engines %v", engine, available)
	}
	return &multiplexer{
		shards:  map[string]Searcher{},
		factory: f,
		params:  params,
	}, nil
}
