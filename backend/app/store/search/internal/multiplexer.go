package internal

import (
	"context"
	"sync/atomic"
	"time"

	log "github.com/go-pkgz/lgr"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"

	"github.com/umputun/remark42/backend/app/store"
	"github.com/umputun/remark42/backend/app/store/engine"
	types "github.com/umputun/remark42/backend/app/store/search/types"
)

// Multiplexer handles search requests siteID to particular engine
type Multiplexer struct {
	shards     map[string]*bufferedEngine
	engineType string
	ready      atomic.Value
}

func newMultiplexer(shards map[string]*bufferedEngine, engineType string) *Multiplexer {
	m := &Multiplexer{
		shards:     shards,
		engineType: engineType,
	}
	m.ready.Store(false)
	return m
}

// IndexDocument adds comment to index
func (s *Multiplexer) IndexDocument(comment *store.Comment) error {
	searcher, has := s.shards[comment.Locator.SiteID]
	if !has {
		return errors.Errorf("no search index for site %q", comment.Locator.SiteID)
	}
	doc := types.DocFromComment(comment)
	return searcher.IndexDocument(doc)
}

const maxErrsDuringStartup = 20

// Init creates missing indexes and index existing documents
func (s *Multiplexer) Init(ctx context.Context, e engine.Interface) error {
	if e == nil {
		s.ready.Store(true)
		return nil
	}

	errs := new(multierror.Error)

	for siteID, seng := range s.shards {
		initialized, err := seng.Init(ctx)
		if err == nil && !initialized {
			err = indexSite(ctx, siteID, e, seng)
			errs = multierror.Append(errs, err)
		}
		errs = multierror.Append(errs, err)

	}
	err := errs.ErrorOrNil()
	if err == nil {
		s.ready.Store(true)
	}
	return err
}

// Ready ensures that seracher is initialized
func (s *Multiplexer) Ready() bool {
	return s.ready.Load().(bool)
}

func indexTopic(ctx context.Context, comments []store.Comment, s indexer) (int, *multierror.Error) {
	errs := new(multierror.Error)

	indexedCnt := 0
	for _, comment := range comments {
		select {
		case <-ctx.Done():
			return indexedCnt, multierror.Append(errs, ctx.Err())
		default:
		}
		comment := comment
		doc := types.DocFromComment(&comment)
		err := s.IndexDocument(doc)
		if err != nil {
			errs = multierror.Append(errs, err)
			continue
		}
		indexedCnt++
	}
	return indexedCnt, errs
}

func indexSite(ctx context.Context, siteID string, e engine.Interface, s indexer) error {
	log.Printf("[INFO] indexing site %q", siteID)
	startTime := time.Now()

	req := engine.InfoRequest{Locator: store.Locator{SiteID: siteID}}
	topics, err := e.Info(req)
	if err != nil {
		return errors.Wrapf(err, "failed to get topics for site %q", siteID)
	}

	errs := new(multierror.Error)
	indexedCnt := 0

	for i := len(topics) - 1; i >= 0; i-- {
		locator := store.Locator{SiteID: siteID, URL: topics[i].URL}
		req := engine.FindRequest{Locator: locator, Since: time.Time{}}
		comments, findErr := e.Find(req)
		if findErr != nil {
			errs = multierror.Append(errs, findErr)
			continue
		}
		cnt, topicErrs := indexTopic(ctx, comments, s)
		indexedCnt += cnt

		if topicErrs != nil {
			errs = multierror.Append(errs, topicErrs.Errors...)
			if errs.Len() >= maxErrsDuringStartup {
				break
			}
		}
	}
	err = errs.ErrorOrNil()
	log.Printf("[INFO] %d documents indexed for site %q in %v. errors: %v",
		indexedCnt, siteID, time.Since(startTime), err)

	return err
}

// Flush documents buffer for site
func (s *Multiplexer) Flush(siteID string) error {
	if !s.Ready() {
		return errors.New("not initialized")
	}
	if inner, has := s.shards[siteID]; has {
		return inner.Flush()
	}
	return errors.Errorf("index for site %q not found", siteID)
}

// Help returns help string
func (s *Multiplexer) Help() string {
	switch s.engineType {
	case "bleve":
		return "See" + " " +
			"<a href=\"http://blevesearch.com/docs/Query-String-Query\">" +
			"blevesearch.com/docs/Query-String-Query</a>" + " " +
			"for help"
	}
	return ""
}

