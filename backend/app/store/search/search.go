// Package search provides full-text search functionality for site
package search

import (
	"context"

	"github.com/umputun/remark42/backend/app/store"
	"github.com/umputun/remark42/backend/app/store/engine"
	"github.com/umputun/remark42/backend/app/store/search/internal"
	types "github.com/umputun/remark42/backend/app/store/search/types"
)

// NOTE: mockery works from linked to go-path and with GOFLAGS='-mod=vendor' go generate
//go:generate sh -c "mockery -inpkg -name Service -print > /tmp/search-mock.tmp && mv /tmp/search-mock.tmp search_mock.go"

// Service provides search for engine
type Service interface {
	IndexDocument(comment *store.Comment) error
	Init(ctx context.Context, e engine.Interface) error
	Ready() bool
	Flush(siteID string) error
	Search(req *types.Request) (*types.ResultPage, error)
	Delete(siteID, commentID string) error
	Help() string
	Close() error
}

// SearcherParams is an alias for types.SearcherParams
type SearcherParams = types.SearcherParams

// ErrSearchNotEnabled occurs when search is not enabled on startup,
// but some method is called
var ErrSearchNotEnabled = types.ErrSearchNotEnabled

// NewSearcher creates new searcher with specified type and parameters
func NewSearcher(params SearcherParams) (Service, error) {
	switch params.Type {
	case "bleve":
		return internal.NewBleveService(params)
	case "elastic":
		return internal.NewElasticService(params)
	}
	return internal.NewNoopService()
}
