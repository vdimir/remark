// Package search provides full-text search functionality for site
package search

import (
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/umputun/remark42/backend/app/store"
	"github.com/umputun/remark42/backend/app/store/search/internal"
	"log"
)

// NOTE: mockery works from linked to go-path and with GOFLAGS='-mod=vendor' go generate
// go:generate sh -c "mockery -inpkg -name Service -print > /tmp/search-mock.tmp && mv /tmp/search-mock.tmp search_mock.go"

// Service provides search
type Service struct {
	shards     map[string]Engine
}

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

// Search document
func (s *Service) Search(req *Request) (*ResultPage, error) {
	if eng, has := s.shards[req.SiteID]; has {
		return eng.Search(req)
	}
	return nil, errors.Errorf("no site %q in index", req.SiteID)
}

func (s *Service) Index(doc *store.Comment) error {
	if eng, has := s.shards[doc.Locator.SiteID]; has {
		return eng.Index(doc)
	}
	return errors.Errorf("Site %q not found", doc.Locator.SiteID)
}

// Delete document from index
func (s *Service) Delete(siteID, commentID string) error {
	if eng, has := s.shards[siteID]; has {
		return eng.Delete(commentID)
	}
	return errors.Errorf("Site %q not found", siteID)
}

func (s *Service) Close() error {

	log.Print("[INFO] closing search service...")
	errs := new(multierror.Error)

	for siteID, searcher := range s.shards {
		if err := searcher.Close(); err != nil {
			errs = multierror.Append(errs, errors.Wrapf(err, "cannot close searcher for %q", siteID))
		}
	}
	log.Print("[INFO] search service closed")
	return errs.ErrorOrNil()
}
/*
type Service interface {
	IndexDocument(comment *store.Comment) error
	Init(ctx context.Context, e engine.Interface) error
	Search(req *types.Request) (*types.ResultPage, error)
	Delete(siteID, commentID string) error
	Help() string
	Close() error
}
*/

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
