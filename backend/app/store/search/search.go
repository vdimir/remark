// Package search provides full-text search functionality for site
package search

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/umputun/remark42/backend/app/store"
)

type indexerBatch interface {
	Index(id string, data interface{}) error
}

type indexer interface {
	NewBatch() indexerBatch
	Batch(batch indexerBatch) error
	Search(req *Request) (*ResultPage, error)
	Delete(id string) error
	Close() error
}

const commentDocType = "docComment"
const urlFieldName = "url"
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

type searchEngine interface {
	IndexDocument(doc *DocumentComment) error
	Search(req *Request) (*ResultPage, error)
	Init(ctx context.Context) (bool, error)
	Delete(id string) error
	Flush() error
	Close() error
}

func newSearchEngine(indexType, indexPath, analyzer string) (s searchEngine, err error) {
	if _, ok := analyzerMapping[analyzer]; !ok {
		analyzers := make([]string, 0, len(analyzerMapping))
		for k := range analyzerMapping {
			analyzers = append(analyzers, k)
		}
		return nil, errors.Errorf("Unknown analyzer: %q. Available analyzers for bleve: %v", analyzer, analyzers)
	}

	if err != nil {
		return nil, errors.Wrap(err, "cannot create/open index")
	}

	var index indexer
	switch indexType {
	case "bleve":
		index, err = newBleveIndexer(indexPath, analyzer)
	default:
		available := []string{"bleve"}
		return nil, errors.Errorf("no search engine %q, available engines %v", indexType, available)
	}

	if err != nil {
		return nil, errors.Wrap(err, "cannot create/open index")
	}

	eng := &searchEngineImpl{
		index:         index,
		queueNotifier: make(chan bool),
		flushEvery:    2 * time.Second,
		flushCount:    100,
		indexPath:     indexPath,
	}

	go eng.indexDocumentWorker()
	return eng, nil
}
