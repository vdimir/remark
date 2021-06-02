package internal

import (
	"encoding/hex"
	"hash/fnv"
	"log"
	"os"
	"path"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/pkg/errors"
	types "github.com/umputun/remark42/backend/app/store/search/types"
)

func newBleve(indexPath, analyzer string) (s *bufferedEngine, err error) {
	if _, ok := analyzerMapping[analyzer]; !ok {
		analyzers := make([]string, 0, len(analyzerMapping))
		for k := range analyzerMapping {
			analyzers = append(analyzers, k)
		}
		return nil, errors.Errorf("Unknown analyzer: %q. Available analyzers for bleve: %v", analyzer, analyzers)
	}
	var index bleve.Index

	st, errOpen := os.Stat(indexPath)
	switch {
	case os.IsNotExist(errOpen):
		// create new
		log.Printf("[INFO] creating new search index %s", indexPath)
		index, err = bleve.New(indexPath, createIndexMapping(analyzerMapping[analyzer]))
		if err != nil {
			return nil, errors.Wrap(err, "cannot open index")
		}
	case errOpen == nil:
		// open existing
		if !st.IsDir() {
			return nil, errors.Errorf("index path should be a directory")
		}
		log.Printf("[INFO] opening existing search index %s", indexPath)
		index, err = bleve.Open(indexPath)
		if err != nil {
			return nil, errors.Wrap(err, "cannot create index")
		}
	default:
		// error
		return nil, errors.Wrap(err, "cannot open index")
	}

	eng := &bufferedEngine{
		index:         bleveIndexer{index},
		queueNotifier: make(chan bool),
		flushEvery:    2 * time.Second,
		flushCount:    100,
		indexPath:     indexPath,
	}

	eng.Start()

	return eng, nil
}

// NewBleveService create search service based on bleve engine
func NewBleveService(params types.SearcherParams) (s *Multiplexer, err error) {
	encodeSiteID := func(siteID string) string {
		h := fnv.New32().Sum([]byte(siteID))
		return hex.EncodeToString(h)
	}

	shards := map[string]*bufferedEngine{}

	for _, siteID := range params.Sites {
		fpath := path.Join(params.IndexPath, encodeSiteID(siteID))
		shards[siteID], err = newBleve(fpath, params.Analyzer)
		if err != nil {
			return nil, err
		}
	}
	return newMultiplexer(shards, params.Type), err
}

// NewBatch creates new empty bleve batch
func (idx bleveIndexer) NewBatch() indexerBatch {
	return bleveBatch{idx.Index.NewBatch()}
}

// Batch indexes whole batch of documents
func (idx bleveIndexer) Batch(batch indexerBatch) error {
	b := batch.(bleveBatch).Batch
	return idx.Index.Batch(b)
}
