package search

import (
	store "github.com/umputun/remark42/backend/app/store"
)

type Engine interface {
	Index(comment *store.Comment) error
	IndexBatch(batch []*store.Comment) error
	Search(req *Request) (*ResultPage, error)
	Delete(id string) error
	Close() error
}
