package search

import (
	"encoding/hex"
	"hash/fnv"
	"path"

	log "github.com/go-pkgz/lgr"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/umputun/remark42/backend/app/store"
)

// SearcherParams parameters to configure engine
type SearcherParams struct {
	IndexPath string
	Analyzer  string
}

type searcherFactory func(string, SearcherParams) (Searcher, error)

// multiplexer handles requests siteID to particular engine
type multiplexer struct {
	shards  map[string]Searcher
	factory searcherFactory
	params  SearcherParams
}

func encodeSiteID(siteID string) string {
	h := fnv.New32().Sum([]byte(siteID))
	return hex.EncodeToString(h)
}

var engineMap map[string]searcherFactory = map[string]searcherFactory{
	"bleve": func(siteID string, p SearcherParams) (Searcher, error) {
		fpath := path.Join(p.IndexPath, encodeSiteID(siteID))
		return newBleveService(fpath, p.Analyzer)
	},
}

// NewSearcher creates new searcher with specified type and parameters
func NewSearcher(engine string, params SearcherParams) (Searcher, error) {
	f, has := engineMap[engine]
	if !has {
		available := []string{}
		for k := range engineMap {
			available = append(available, k)
		}
		return nil, errors.Errorf("no search engine %q, available engines %v", engine, available)
	}
	return &multiplexer{
		shards:  map[string]Searcher{},
		factory: f,
		params:  params,
	}, nil
}

func (s *multiplexer) IndexDocument(commentID string, comment *store.Comment) error {
	searcher, err := s.getOrCreate(comment.Locator.SiteID)
	if err != nil {
		return err
	}
	return searcher.IndexDocument(commentID, comment)
}

func (s *multiplexer) Search(req *Request) (*ResultPage, error) {
	searcher, err := s.getOrCreate(req.SiteID)
	if err != nil {
		return nil, err
	}
	return searcher.Search(req)

}

func (s *multiplexer) Close() error {
	errs := new(multierror.Error)

	for siteID, searcher := range s.shards {
		if err := searcher.Close(); err != nil {
			errs = multierror.Append(errs, errors.Wrapf(err, "cannot close searcher for %q", siteID))
		}
	}
	return errs.ErrorOrNil()
}

func (s *multiplexer) getOrCreate(siteID string) (Searcher, error) {
	searcher, has := s.shards[siteID]
	if !has {
		log.Printf("[INFO] creating new searcher for site %q", siteID)
		var err error
		searcher, err = s.factory(siteID, s.params)
		if err != nil {
			return nil, err
		}
		s.shards[siteID] = searcher
		return searcher, err
	}
	return searcher, nil
}
