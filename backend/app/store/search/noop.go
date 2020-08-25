package search

import (
	"context"

	"github.com/pkg/errors"

	"github.com/umputun/remark42/backend/app/store"
	"github.com/umputun/remark42/backend/app/store/engine"
)

// ErrSearchNotEnabled returned to search request in case search not enabled
var ErrSearchNotEnabled = errors.New("search not enabled")

type noopSearchService struct{}

func newNoopService() (Service, error) {
	return &noopSearchService{}, nil
}

func (*noopSearchService) IndexDocument(comment *store.Comment) error {
	return nil
}

func (*noopSearchService) Init(ctx context.Context, e engine.Interface) error {
	return nil
}

func (*noopSearchService) Ready() bool {
	return true
}

func (*noopSearchService) Flush(siteID string) error {
	return nil
}

func (*noopSearchService) Search(req *Request) (*ResultPage, error) {
	return nil, ErrSearchNotEnabled
}

func (*noopSearchService) Delete(siteID, commentID string) error {
	return nil
}

func (*noopSearchService) Help() string {
	return "search not enabled"
}

func (*noopSearchService) Close() error {
	return nil
}
