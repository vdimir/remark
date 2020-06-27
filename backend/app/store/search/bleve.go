package search

import (
	"fmt"
	"os"
	"time"

	log "github.com/go-pkgz/lgr"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/mapping"
	"github.com/gammazero/deque"
	"github.com/pkg/errors"
	"github.com/umputun/remark42/backend/app/store"

	"github.com/blevesearch/bleve/analysis/analyzer/keyword"
	bleveStandard "github.com/blevesearch/bleve/analysis/analyzer/standard"
	bleveEn "github.com/blevesearch/bleve/analysis/lang/en"
	bleveRu "github.com/blevesearch/bleve/analysis/lang/ru"
)

// bleveEngine provides search using bleve library
type bleveEngine struct {
	docQueue      deque.Deque
	queueNotifier chan bool
	index         bleve.Index
	flushEvery    time.Duration
	flushCount    int
}

const commentDocType = "docComment"
const urlFiledName = "url"

// Available text analyzers.
// Bleve supports a bit more languages that may be added,
// see https://github.com/blevesearch/bleve/tree/master/analysis/lang
var analyzerMapping = map[string]string{
	"standard": bleveStandard.Name,
	"en":       bleveEn.AnalyzerName,
	"ru":       bleveRu.AnalyzerName,
}

// DocumentComment is document describes comment stored in index
// Bridge between store.Comment and Bleve index
type DocumentComment struct {
	ID        string    `json:"id"`
	URL       string    `json:"url"`
	Text      string    `json:"text"`
	Timestamp time.Time `json:"timestamp"`
	UserName  string    `json:"username"`
}

// DocFromComment converts store.Comment to DocumentComment
func DocFromComment(comment *store.Comment) *DocumentComment {
	return &DocumentComment{
		URL:       comment.Locator.URL,
		ID:        comment.ID,
		Text:      comment.Text,
		Timestamp: comment.Timestamp,
		UserName:  comment.User.Name,
	}
}

// Type implements bleve.Classifier
func (d DocumentComment) Type() string {
	return commentDocType
}

type idxFlusher struct {
	notifier chan struct{}
}

// newBleveService returns new bleveEngine instance
func newBleveService(indexPath, analyzer string) (s searchEngine, opened bool, err error) {
	if _, ok := analyzerMapping[analyzer]; !ok {
		analyzers := make([]string, 0, len(analyzerMapping))
		for k := range analyzerMapping {
			analyzers = append(analyzers, k)
		}
		return nil, opened, errors.Errorf("Unknown analyzer: %q. Available analyzers for bleve: %v", analyzer, analyzers)
	}
	var index bleve.Index

	if _, errOpen := os.Stat(indexPath); os.IsNotExist(errOpen) {
		log.Printf("[INFO] creating new search index %s", indexPath)
		index, err = bleve.New(indexPath, createIndexMapping(analyzerMapping[analyzer]))
	} else if errOpen == nil {
		log.Printf("[INFO] opening existing search index %s", indexPath)
		index, err = bleve.Open(indexPath)
		opened = true
	} else {
		err = errOpen
	}

	if err != nil {
		return nil, opened, errors.Wrap(err, "cannot create/open index")
	}

	eng := &bleveEngine{
		index:         index,
		queueNotifier: make(chan bool),
		flushEvery:    2 * time.Second,
		flushCount:    100,
	}

	go eng.indexDocumentWorker()
	return eng, opened, nil
}

// IndexDocument adds or updates document to search index
func (s *bleveEngine) IndexDocument(commentID string, comment *store.Comment) error {
	doc := DocFromComment(comment)
	log.Printf("[DEBUG] index document %s", commentID)
	s.docQueue.PushBack(doc)
	s.queueNotifier <- false
	return nil
}

func (s *bleveEngine) indexBatch() {
	docCount := s.docQueue.Len()
	if docCount == 0 {
		return
	}

	batch := s.index.NewBatch()
	for i := 0; i < docCount; i++ {
		switch val := s.docQueue.PopFront().(type) {
		case *DocumentComment:
			err := batch.Index(val.ID, val)
			if err != nil {
				log.Printf("[ERROR] error while adding doc %q to batch %v", val.ID, err)
				break
			}
		case *idxFlusher:
			defer func() { val.notifier <- struct{}{} }()
		default:
			panic(fmt.Sprintf("unknown type %T", val))
		}
	}
	err := s.index.Batch(batch)
	log.Printf("[ERROR] error while indexing batch, %v", err)
}

func (s *bleveEngine) indexDocumentWorker() {
	log.Printf("[INFO] start bleve indexer worker")
	tmr := time.NewTimer(s.flushEvery)
	cont := true
	for cont {
		var force bool
		select {
		case <-tmr.C:
			s.indexBatch()
			tmr.Reset(s.flushEvery)
		case force, cont = <-s.queueNotifier:
			full := s.docQueue.Len() >= s.flushCount
			if force || full {
				s.indexBatch()
			}
		}
	}
	// TODO(@vdimir) save current buffer
	log.Printf("[INFO] shutdown bleve indexer worker")
}

// Flush documents buffer
func (s *bleveEngine) Flush() {
	flusher := &idxFlusher{make(chan struct{})}
	s.docQueue.PushBack(flusher)
	s.queueNotifier <- true

	<-flusher.notifier
}

func createIndexMapping(textAnalyzer string) mapping.IndexMapping {
	indexMapping := bleve.NewIndexMapping()

	indexMapping.AddDocumentMapping(commentDocType, commentDocumentMapping(textAnalyzer))

	return indexMapping
}

func textMapping(analyzer string, doStore bool) *mapping.FieldMapping {
	textFieldMapping := bleve.NewTextFieldMapping()
	textFieldMapping.Store = doStore
	textFieldMapping.Analyzer = analyzer
	return textFieldMapping
}

func commentDocumentMapping(textAnalyzer string) *mapping.DocumentMapping {
	commentMapping := bleve.NewDocumentMapping()

	commentMapping.AddFieldMappingsAt("text", textMapping(textAnalyzer, false))
	commentMapping.AddFieldMappingsAt("username", textMapping(keyword.Name, true))
	commentMapping.AddFieldMappingsAt("urlFiledName", textMapping(keyword.Name, true))

	return commentMapping
}

// Search documents
func (s *bleveEngine) Search(req *Request) (*ResultPage, error) {
	log.Printf("[INFO] searching %v", req)

	bQuery := bleve.NewQueryStringQuery(req.Query)
	bReq := bleve.NewSearchRequestOptions(bQuery, req.Limit, req.From, false)

	if validateSortField(req.SortBy, "timestamp") {
		bReq.SortBy([]string{req.SortBy})
	} else if req.SortBy != "" {
		log.Printf("[WARN] unknown sort field %q", req.SortBy)
	}

	bReq.Fields = append(bReq.Fields, urlFiledName)

	serp, err := s.index.Search(bReq)
	if err != nil {
		return nil, errors.Wrap(err, "search error")
	}
	log.Printf("[INFO] found %d documents for query %q in %s",
		serp.Total, req.Query, serp.Took.String())

	result := convertBleveSerp(serp)
	return result, nil
}

func convertBleveSerp(bleveResult *bleve.SearchResult) *ResultPage {
	result := ResultPage{
		Total:     bleveResult.Total,
		Documents: make([]ResultDoc, 0, len(bleveResult.Hits)),
	}
	for _, r := range bleveResult.Hits {
		url, hasURL := r.Fields[urlFiledName].(string)
		if !hasURL {
			panic(fmt.Sprintf("cannot find %q in %v", urlFiledName, r.Fields))
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

// Delete comment from index
func (s *bleveEngine) Delete(commentID string) error {
	if err := s.index.Delete(commentID); err != nil {
		return errors.Wrapf(err, "cannot detele comment %q from search index", commentID)
	}
	return nil
}

// Close search service
func (s *bleveEngine) Close() error {
	close(s.queueNotifier)
	return s.index.Close()
}

func validateSortField(sortBy string, possible ...string) bool {
	if sortBy == "" {
		return false
	}
	if sortBy[0] == '-' || sortBy[0] == '+' {
		sortBy = sortBy[1:]
	}
	for _, e := range possible {
		if sortBy == e {
			return true
		}
	}
	return false
}
