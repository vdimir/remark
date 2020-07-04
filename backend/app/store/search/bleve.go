package search

import (
	"fmt"
	"os"

	log "github.com/go-pkgz/lgr"

	"github.com/blevesearch/bleve"
	bleveCustom "github.com/blevesearch/bleve/analysis/analyzer/custom"
	bleveStandard "github.com/blevesearch/bleve/analysis/analyzer/standard"
	bleveEn "github.com/blevesearch/bleve/analysis/lang/en"
	bleveRu "github.com/blevesearch/bleve/analysis/lang/ru"
	bleveSingle "github.com/blevesearch/bleve/analysis/tokenizer/single"

	"github.com/blevesearch/bleve/analysis/token/lowercase"
	"github.com/blevesearch/bleve/mapping"
	"github.com/pkg/errors"
)

// Available text analyzers.
// Bleve supports a bit more languages that may be added,
// see https://github.com/blevesearch/bleve/tree/master/analysis/lang
var analyzerMapping = map[string]string{
	"standard": bleveStandard.Name,
	"en":       bleveEn.AnalyzerName,
	"ru":       bleveRu.AnalyzerName,
}

type bleveIndexer struct {
	bleve.Index
}

func (idx bleveIndexer) NewBatch() indexerBatch {
	return idx.Index.NewBatch()
}

func (idx bleveIndexer) Batch(batch indexerBatch) error {
	b := batch.(*bleve.Batch)
	return idx.Index.Batch(b)
}

func convertBleveSerp(bleveResult *bleve.SearchResult) *ResultPage {
	result := ResultPage{
		Total:     bleveResult.Total,
		Documents: make([]ResultDoc, 0, len(bleveResult.Hits)),
	}
	for _, r := range bleveResult.Hits {
		url, hasURL := r.Fields[urlFieldName].(string)
		if !hasURL {
			panic(fmt.Sprintf("cannot find %q in %v", urlFieldName, r.Fields))
		}

		d := ResultDoc{
			ID:      r.ID,
			Matches: make([]TokenMatch, len(r.FieldTermLocations)),
			PostURL: url,
		}
		for _, loc := range r.FieldTermLocations {
			d.Matches = append(d.Matches, TokenMatch{
				Start: loc.Location.Start,
				End:   loc.Location.End,
			})
		}
		result.Documents = append(result.Documents, d)
	}
	return &result
}

func (idx bleveIndexer) Search(req *Request) (*ResultPage, error) {
	bQuery := bleve.NewQueryStringQuery(req.Query)
	bReq := bleve.NewSearchRequestOptions(bQuery, req.Limit, req.From, false)

	if validateSortField(req.SortBy, "timestamp") {
		bReq.SortBy([]string{req.SortBy})
	} else if req.SortBy != "" {
		log.Printf("[WARN] unknown sort field %q", req.SortBy)
	}

	bReq.Fields = append(bReq.Fields, urlFieldName)

	serp, err := idx.Index.Search(bReq)
	if err != nil {
		return nil, errors.Wrap(err, "search error")
	}
	log.Printf("[INFO] found %d documents for query %q in %s",
		serp.Total, req.Query, serp.Took.String())

	result := convertBleveSerp(serp)
	return result, nil
}

func (idx bleveIndexer) Delete(id string) error {
	return idx.Index.Delete(id)
}

func (idx bleveIndexer) Close() error {
	return idx.Index.Close()
}

// newBleveIndexer returns new bleveEngine instance
func newBleveIndexer(indexPath, analyzer string) (s indexer, err error) {
	if _, ok := analyzerMapping[analyzer]; !ok {
		analyzers := make([]string, 0, len(analyzerMapping))
		for k := range analyzerMapping {
			analyzers = append(analyzers, k)
		}
		return nil, errors.Errorf("Unknown analyzer: %q. Available analyzers for bleve: %v", analyzer, analyzers)
	}
	var index bleve.Index

	if st, errOpen := os.Stat(indexPath); os.IsNotExist(errOpen) {
		log.Printf("[INFO] creating new search index %s", indexPath)
		index, err = bleve.New(indexPath, createIndexMapping(analyzerMapping[analyzer]))
	} else if errOpen == nil {
		if !st.IsDir() {
			return nil, errors.Errorf("index path shoule be a directory")
		}
		log.Printf("[INFO] opening existing search index %s", indexPath)
		index, err = bleve.Open(indexPath)
	} else {
		err = errOpen
	}

	if err != nil {
		return nil, errors.Wrap(err, "cannot create/open index")
	}

	return bleveIndexer{index}, nil
}

func createIndexMapping(textAnalyzer string) mapping.IndexMapping {
	indexMapping := bleve.NewIndexMapping()
	err := indexMapping.AddCustomAnalyzer("keyword_lower", map[string]interface{}{
		"type":      bleveCustom.Name,
		"tokenizer": bleveSingle.Name,
		"token_filters": []string{
			lowercase.Name,
		},
	})
	if err != nil {
		panic(fmt.Sprintf("error adding bleve analyzer %v", err))
	}
	indexMapping.AddDocumentMapping(commentDocType, commentDocumentMapping(textAnalyzer))

	return indexMapping
}
