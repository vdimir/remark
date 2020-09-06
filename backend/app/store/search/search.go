// Package search provides full-text search functionality for site
package search

import (
	"github.com/umputun/remark42/backend/app/store/search/internal"
	service "github.com/umputun/remark42/backend/app/store/search/service"
)

// NewSearcher creates new searcher with specified type and parameters
func NewSearcher(params service.SearcherParams) (service.Service, error) {
	switch params.Type {
	case "bleve":
		return internal.NewBleveService(params)
	case "elastic":
		return internal.NewElasticService(params)
	}
	return internal.NewNoopService()
}
