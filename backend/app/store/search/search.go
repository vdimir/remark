// Package search provides full-text search functionality for site
package search

import (
	"github.com/umputun/remark42/backend/app/store"
)

// Request is the input for Search
type Request struct {
	SiteID string
	Query  string
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
	Close() error
}
