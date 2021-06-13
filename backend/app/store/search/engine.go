package search

import (
	"github.com/pkg/errors"
	"github.com/umputun/remark42/backend/app/store"
)

type Engine interface {
	Index(comments []*store.Comment) error
	Search(req *Request) (*ResultPage, error)
	Delete(id string) error
	Close() error
}

func newEngine(params ServiceParams) (Engine, error) {
	if params.Engine == "bleve" {
		return newBleveEngine(params.IndexPath, params.Analyzer)
	}
	return nil, errors.Errorf("unknown search engine %q", params.Engine)
}
