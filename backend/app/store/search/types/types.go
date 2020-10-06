// Package types is aimed to provide common types
// for `search/internal` and main `search` module
// to avoid circular module dependencies
package types

import (
	"errors"
	"time"

	"github.com/umputun/remark42/backend/app/store"
)

// ErrSearchNotEnabled returned to search request in case search not enabled
var ErrSearchNotEnabled = errors.New("search not enabled")

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
	Documents []ResultDoc `json:"documents"`
}

// SearcherParams parameters to configure engine
type SearcherParams struct {
	Type      string
	IndexPath string
	Analyzer  string
	Sites     []string
	Endpoint  string
	Secret    string
}

// DocumentComment is document that describes comment stored in index
// Bridge between store.Comment and Bleve index
type DocumentComment struct {
	ID        string    `json:"id"`
	SiteID    string    `json:"site"`
	URL       string    `json:"url"`
	Text      string    `json:"text"`
	Timestamp time.Time `json:"timestamp"`
	UserName  string    `json:"username"`
}

const commentDocType = "docComment"

// Type implements bleve.Classifier
func (d DocumentComment) Type() string {
	return commentDocType
}

// DocFromComment converts store.Comment to DocumentComment
func DocFromComment(comment *store.Comment) *DocumentComment {
	return &DocumentComment{
		URL:       comment.Locator.URL,
		SiteID:    comment.Locator.SiteID,
		ID:        comment.ID,
		Text:      comment.Text,
		Timestamp: comment.Timestamp,
		UserName:  comment.User.Name,
	}
}
