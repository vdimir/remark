// Package search provides full-text search functionality for site
package search

import (
	"time"

	"github.com/umputun/remark42/backend/app/store"
)

type indexerBatch interface {
	Index(id string, data *DocumentComment) error
}

type searchEngine interface {
	NewBatch() indexerBatch
	Batch(batch indexerBatch) error
	Search(req *Request) (*ResultPage, error)
	Delete(id string) error
	Close() error
}

type indexer interface {
	IndexDocument(doc *DocumentComment) error
}

const commentDocType = "docComment"
const aheadLogFname = ".ahead.log"

// DocumentComment is document describes comment stored in index
// Bridge between store.Comment and Bleve index
type DocumentComment struct {
	ID        string    `json:"id"`
	SiteID    string    `json:"site"`
	URL       string    `json:"url"`
	Text      string    `json:"text"`
	Timestamp time.Time `json:"timestamp"`
	UserName  string    `json:"username"`
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

// Type implements bleve.Classifier
func (d DocumentComment) Type() string {
	return commentDocType
}
