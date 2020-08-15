package search

import (
	"context"

	"github.com/pkg/errors"
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

// Service provides search for engine
type Service interface {
	IndexDocument(comment *store.Comment) error
	Init(ctx context.Context, e engine.Interface) error
	Ready() bool
	Flush(siteID string) error
	Search(req *Request) (*ResultPage, error)
	Delete(siteID, commentID string) error
	Type() string
	Close() error
}

// NewSearcher creates new searcher with specified type and parameters
func NewSearcher(params SearcherParams) (Service, error) {
	switch params.Type {
	case "bleve":
		return newBleveService(params)
	case "elastic":
		return newElasticService(params)
	}
	available := []string{"bleve"}
	return nil, errors.Errorf("no search engine %q, available engines %v", params.Type, available)
}

// Help returns text doc for query language
func Help(engineType string) string {
	switch engineType {
	case "bleve":
		return "See" + " " +
			"<a href=\"http://blevesearch.com/docs/Query-String-Query\">" +
			"blevesearch.com/docs/Query-String-Query</a>" + " " +
			"for help"
	}
	return ""
}
