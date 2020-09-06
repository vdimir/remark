package internal

import (
	"context"

	"github.com/pkg/errors"

	"github.com/umputun/remark42/backend/app/store"
	"github.com/umputun/remark42/backend/app/store/engine"
	service "github.com/umputun/remark42/backend/app/store/search/service"
)

// ErrSearchNotEnabled returned to search request in case search not enabled
var ErrSearchNotEnabled = errors.New("search not enabled")

type noopSearchService struct{}

// NewNoopService creates dummy search service
func NewNoopService() (service.Service, error) {
	return &noopSearchService{}, nil
}

// IndexDocument does nothing on noop search service
func (*noopSearchService) IndexDocument(comment *store.Comment) error {
	return nil
}

// Init does nothing on noop search service
func (*noopSearchService) Init(ctx context.Context, e engine.Interface) error {
	return nil
}

// Ready for noop search service always true
func (*noopSearchService) Ready() bool {
	return true
}

// Flush does nothing on noop search service
func (*noopSearchService) Flush(siteID string) error {
	return nil
}

// Search always returns ErrSearchNotEnabled
func (*noopSearchService) Search(req *service.Request) (*service.ResultPage, error) {
	return nil, ErrSearchNotEnabled
}

// Delete does nothing on noop search service
func (*noopSearchService) Delete(siteID, commentID string) error {
	return nil
}

// Help for noop search service
func (*noopSearchService) Help() string {
	return "search not enabled"
}

// Close does nothing on noop search service
func (*noopSearchService) Close() error {
	return nil
}
