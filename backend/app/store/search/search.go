// Package search provides full-text search functionality for site
package search

import (
	"github.com/umputun/remark42/backend/app/store"
	"github.com/umputun/remark42/backend/app/store/engine"
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

// searchEngine provides core interface for search engines
type searchEngine interface {
	IndexDocument(commentID string, comment *store.Comment) error
	Init(e engine.Interface) error
	Flush() error
	Search(req *Request) (*ResultPage, error)
	Delete(commentID string) error

	Close() error
}
