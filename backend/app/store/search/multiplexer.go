package search

import (
	"context"
	"sync/atomic"
	"time"

	log "github.com/go-pkgz/lgr"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/umputun/remark42/backend/app/store"
	"github.com/umputun/remark42/backend/app/store/engine"
)

// multiplexer handles search requests siteID to particular engine
type multiplexer struct {
	shards     map[string]searchEngine
	engineType string
	ready      atomic.Value
}

// IndexDocument adds comment to index
func (s *multiplexer) IndexDocument(commentID string, comment *store.Comment) error {
	searcher, has := s.shards[comment.Locator.SiteID]
	if !has {
		return errors.Errorf("no search index for site %q", comment.Locator.SiteID)
	}
	doc := DocFromComment(comment)
	log.Printf("[DEBUG] index document %s", commentID)
	return searcher.IndexDocument(doc)
}

const maxErrsDuringStartup = 20

// Init creates missing indexes and index existing documents
func (s *multiplexer) Init(ctx context.Context, e engine.Interface) error {
	/* TODO(@vdimir)
	 * This impmlementation could leave index inconsistent with storage in some rare cases.
	 * Consider this situation:
	 * Some comment retrieved from storage during coldstart and had changed,
	 * but changed version indexed before initial that is stored in DB.
	 * So initial version would rewrite changes.
	 */
	if e == nil {
		s.ready.Store(true)
		return nil
	}

	sites, err := e.ListSites()
	if err != nil {
		return err
	}
	errs := new(multierror.Error)

	for _, siteID := range sites {
		seng := s.shards[siteID]
		var initialized bool
		initialized, err = seng.Init(ctx)
		if err == nil && !initialized {
			_, err = indexSite(ctx, siteID, e, seng)
			errs = multierror.Append(errs, err)
		}
		errs = multierror.Append(errs, err)

	}
	err = errs.ErrorOrNil()
	if err == nil {
		s.ready.Store(true)
	}
	return err
}

func (s *multiplexer) Ready() bool {
	return s.ready.Load().(bool)
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
				doc := DocFromComment(&comment)
				if err = s.IndexDocument(doc); err == nil {
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

// Flush documents buffer for site
// If `Flush` called before `Init` or after `Init` that ends with error it blocks forever
func (s *multiplexer) Flush(siteID string) error {
	for {
		if s.Ready() {
			break
		}
		<-time.After(10 * time.Second)
	}
	if inner, has := s.shards[siteID]; has {
		return inner.Flush()
	}
	return errors.Errorf("index for site %q not found", siteID)
}

// Search document
func (s *multiplexer) Search(req *Request) (*ResultPage, error) {
	searcher, has := s.shards[req.SiteID]
	if !has {
		return nil, errors.Errorf("no site %q in index", req.SiteID)
	}
	return searcher.Search(req)
}

// Delete document from index
func (s *multiplexer) Delete(siteID, commentID string) error {
	if inner, has := s.shards[siteID]; has {
		return inner.Delete(commentID)
	}
	return nil
}

// Type return engine type
func (s *multiplexer) Type() string {
	return s.engineType
}

// Close releases resources
func (s *multiplexer) Close() error {
	log.Print("[INFO] closing search service...")
	errs := new(multierror.Error)

	for siteID, searcher := range s.shards {
		if err := searcher.Close(); err != nil {
			errs = multierror.Append(errs, errors.Wrapf(err, "cannot close searcher for %q", siteID))
		}
	}
	return errs.ErrorOrNil()
}
