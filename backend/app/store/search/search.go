// Package search provides full-text search functionality for site
package search

import (
	"context"

	"github.com/umputun/remark42/backend/app/store"
	"github.com/umputun/remark42/backend/app/store/engine"
	"github.com/umputun/remark42/backend/app/store/search/internal"
	types "github.com/umputun/remark42/backend/app/store/search/types"
)

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

type SearcherParams = types.SearcherParams

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
