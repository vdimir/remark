package internal

import (
	"context"

	"github.com/umputun/remark42/backend/app/store"
	"github.com/umputun/remark42/backend/app/store/engine"
	types "github.com/umputun/remark42/backend/app/store/search/types"
)

// NoopSearchService is a dummy searcher
type NoopSearchService struct{}

// NewNoopService creates dummy search service
func NewNoopService() (*NoopSearchService, error) {
	return &NoopSearchService{}, nil
}

// IndexDocument does nothing on noop search service
func (*NoopSearchService) IndexDocument(comment *store.Comment) error {
	return nil
}

// Init does nothing on noop search service
func (*NoopSearchService) Init(ctx context.Context, e engine.Interface) error {
	return nil
}

// Ready for noop search service always true
func (*NoopSearchService) Ready() bool {
	return true
}

// Flush does nothing on noop search service
func (*NoopSearchService) Flush(siteID string) error {
	return nil
}

// Search always returns ErrSearchNotEnabled
func (*NoopSearchService) Search(req *types.Request) (*types.ResultPage, error) {
	return nil, types.ErrSearchNotEnabled
}

// Delete does nothing on noop search service
func (*NoopSearchService) Delete(siteID, commentID string) error {
	return nil
}

// Help for noop search service
func (*NoopSearchService) Help() string {
	return ""
}

// Close does nothing on noop search service
func (*NoopSearchService) Close() error {
	return nil
}
