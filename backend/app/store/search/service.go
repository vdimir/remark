package search

import (
	"context"
	"encoding/hex"
	"hash/fnv"
	"path"
	"time"

	log "github.com/go-pkgz/lgr"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/umputun/remark42/backend/app/store"
	"github.com/umputun/remark42/backend/app/store/engine"
)

// SearcherParams parameters to configure engine
type SearcherParams struct {
	IndexPath string
	Analyzer  string
	Sites     []string
}

// Service handles search requests siteID to particular engine
type Service struct {
	shards        map[string]searchEngine
	existedShards map[string]bool
	ready         bool
}

// NotReadyError occurs on search in not ready engine
type NotReadyError struct {
}

func (NotReadyError) Error() string {
	return "search engine not ready"
}

// NewSearcher creates new searcher with specified type and parameters
func NewSearcher(engineType string, params SearcherParams) (*Service, error) {
	encodeSiteID := func(siteID string) string {
		h := fnv.New32().Sum([]byte(siteID))
		return hex.EncodeToString(h)
	}

	existedShards := map[string]bool{}
	shards := map[string]searchEngine{}
	var err error

	for _, siteID := range params.Sites {
		switch engineType {
		case "bleve":
			fpath := path.Join(params.IndexPath, encodeSiteID(siteID))
			shards[siteID], existedShards[siteID], err = newBleveService(fpath, params.Analyzer)
			if err != nil {
				return nil, err
			}
		default:
			available := []string{"bleve"}
			return nil, errors.Errorf("no search engine %q, available engines %v", engineType, available)
		}
	}

	ready := true
	for _, siteID := range params.Sites {
		ready = ready && existedShards[siteID]
	}

	return &Service{
		shards:        shards,
		existedShards: existedShards,
		ready:         ready,
	}, err
}

// IndexDocument adds comment to index
func (s *Service) IndexDocument(commentID string, comment *store.Comment) error {
	searcher, has := s.shards[comment.Locator.SiteID]
	if !has {
		return errors.Errorf("no search index for site %q", comment.Locator.SiteID)
	}
	return searcher.IndexDocument(commentID, comment)
}

// Search document
func (s *Service) Search(req *Request) (*ResultPage, error) {
	if !s.ready {
		return nil, NotReadyError{}
	}
	searcher, has := s.shards[req.SiteID]
	if !has {
		return nil, errors.Errorf("no site %q in index", req.SiteID)
	}
	return searcher.Search(req)

}

// Delete document from index
func (s *Service) Delete(siteID, commentID string) error {
	if inner, has := s.shards[siteID]; has {
		return inner.Delete(commentID)
	}
	return nil
}

const maxErrsDuringStartup = 20

// PrepareColdstart creates missing indexes and index existing documents
func (s *Service) PrepareColdstart(ctx context.Context, e engine.Interface) error {
	if s.ready {
		log.Printf("[INFO] index already ready for all sites")
		return nil
	}

	sites, err := e.ListSites()
	if err != nil {
		return err
	}
	errs := new(multierror.Error)
	indexedCnt := 0
	for _, siteID := range sites {
		exists := s.existedShards[siteID]
		if exists {
			log.Printf("[INFO] index for site %q exists", siteID)
			continue
		}
		indexedInSite, err := indexSite(ctx, siteID, e, s.shards[siteID])
		indexedCnt += indexedInSite
		errs = multierror.Append(errs, err)
	}
	if errs.Len() <= maxErrsDuringStartup {
		s.ready = true
	}
	return errs.ErrorOrNil()
}

// IsReady returns true if initialization is finished
func (s *Service) IsReady() bool {
	return s.ready
}

func indexSite(ctx context.Context, siteID string, e engine.Interface, s searchEngine) (int, error) {
	log.Printf("[INFO] indexing site %q", siteID)

	req := engine.InfoRequest{Locator: store.Locator{SiteID: siteID}}
	topics, err := e.Info(req)
	if err != nil {
		return 0, err
	}

	errs := new(multierror.Error)
	indexedCnt := 0
	for i := len(topics) - 1; i >= 0; i-- {
		topic := topics[i]
		locator := store.Locator{SiteID: siteID, URL: topic.URL}
		req := engine.FindRequest{Locator: locator, Since: time.Time{}}
		comments, err := e.Find(req)
		if err == nil {
			for _, comment := range comments {
				select {
				case <-ctx.Done():
					return indexedCnt, multierror.Append(errs, ctx.Err()).ErrorOrNil()
				default:
				}
				comment := comment
				if err = s.IndexDocument(comment.ID, &comment); err == nil {
					indexedCnt++
				}
			}
		}
		if err == nil {
			continue
		}
		if errs = multierror.Append(errs, err); errs.Len() >= maxErrsDuringStartup {
			return indexedCnt, errs.ErrorOrNil()
		}
	}
	return indexedCnt, errs.ErrorOrNil()
}

// Close releases resources
func (s *Service) Close() error {
	errs := new(multierror.Error)

	for siteID, searcher := range s.shards {
		if err := searcher.Close(); err != nil {
			errs = multierror.Append(errs, errors.Wrapf(err, "cannot close searcher for %q", siteID))
		}
	}
	return errs.ErrorOrNil()
}
