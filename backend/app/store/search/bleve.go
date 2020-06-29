package search

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	log "github.com/go-pkgz/lgr"

	"github.com/blevesearch/bleve"
	bleveCustom "github.com/blevesearch/bleve/analysis/analyzer/custom"
	bleveStandard "github.com/blevesearch/bleve/analysis/analyzer/standard"
	bleveEn "github.com/blevesearch/bleve/analysis/lang/en"
	bleveRu "github.com/blevesearch/bleve/analysis/lang/ru"
	"github.com/blevesearch/bleve/analysis/token/lowercase"
	bleveSingle "github.com/blevesearch/bleve/analysis/tokenizer/single"
	"github.com/blevesearch/bleve/mapping"
	"github.com/gammazero/deque"
	"github.com/pkg/errors"
	"github.com/umputun/remark42/backend/app/store"
	"github.com/umputun/remark42/backend/app/store/engine"
)

// bleveEngine provides search using bleve library
type bleveEngine struct {
	queueLock     sync.RWMutex
	docQueue      deque.Deque
	queueNotifier chan bool
	index         bleve.Index
	flushEvery    time.Duration
	flushCount    int
	indexPath     string
}

const commentDocType = "docComment"
const urlFieldName = "url"
const aheadLogFname = ".ahead.log"

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
	SiteID    string    `json:"site"`
	URL       string    `json:"url"`
	Text      string    `json:"text"`
	Timestamp time.Time `json:"timestamp"`
	UserName  string    `json:"username"`
}

// DocFromComment converts store.Comment to DocumentComment
func DocFromComment(comment *store.Comment) *DocumentComment {
	return &DocumentComment{
		URL:       comment.Locator.URL,
		SiteID:    comment.Locator.SiteID,
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
	notifier chan error
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

	if st, errOpen := os.Stat(indexPath); os.IsNotExist(errOpen) {
		log.Printf("[INFO] creating new search index %s", indexPath)
		index, err = bleve.New(indexPath, createIndexMapping(analyzerMapping[analyzer]))
	} else if errOpen == nil {
		if !st.IsDir() {
			return nil, opened, errors.Errorf("index path shoule be a directory")
		}
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
		indexPath:     indexPath,
	}

	go eng.indexDocumentWorker()
	return eng, opened, nil
}

// IndexDocument adds or updates document to search index
func (s *bleveEngine) IndexDocument(commentID string, comment *store.Comment) error {
	doc := DocFromComment(comment)
	log.Printf("[DEBUG] index document %s", commentID)
	return s.indexDoc(doc)
}

func (s *bleveEngine) indexDoc(doc *DocumentComment) error {
	s.queueLock.Lock()
	s.docQueue.PushBack(doc)
	s.queueLock.Unlock()
	s.queueNotifier <- false
	return nil
}

func (s *bleveEngine) indexBatch() {
	s.queueLock.Lock()

	docCount := s.docQueue.Len()
	if docCount == 0 {
		s.queueLock.Unlock()
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
			defer func() { val.notifier <- nil }()
		default:
			s.queueLock.Unlock()
			panic(fmt.Sprintf("unknown type %T", val))
		}
	}

	s.queueLock.Unlock()

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
			s.queueLock.RLock()
			full := s.docQueue.Len() >= s.flushCount
			s.queueLock.RUnlock()
			if force || full {
				s.indexBatch()
			}
		}
	}
	log.Printf("[INFO] shutdown bleve indexer worker")

	s.writeAheadLog()
}

func (s *bleveEngine) getAheadLogPath() string {
	return path.Join(s.indexPath, aheadLogFname)
}

func (s *bleveEngine) writeAheadLog() {
	var err error

	aheadLogPath := s.getAheadLogPath()
	f, err := os.Create(filepath.Clean(aheadLogPath))
	if err != nil {
		log.Printf("[ERROR] error %v opening log file %q", err, aheadLogPath)
		return
	}
	defer func() {
		errClose := f.Close()
		if errClose != nil {
			log.Printf("[ERROR] error %v closing log file %q", errClose, aheadLogPath)
		}
	}()

	s.queueLock.Lock()
	defer s.queueLock.Unlock()
	for s.docQueue.Len() > 0 {
		switch val := s.docQueue.PopFront().(type) {
		case *DocumentComment:
			if err != nil {
				continue
			}
			var data []byte
			data, err = json.Marshal(val)
			if err != nil {
				continue
			}
			data = append(data, 0x0)
			_, err = f.Write(data)
		case *idxFlusher:
			defer func() { val.notifier <- errors.Errorf("indexer closing") }()
		default:
			panic(fmt.Sprintf("unknown type %T", val))
		}
	}
	if err != nil {
		log.Printf("[ERROR] error %v writing log file", err)
	}
}

// Init indexer. It loads unindexed comments from ahead log saved from buffer on shutdown
func (s *bleveEngine) Init(e engine.Interface) error {
	// TODO(@vdimir) add tests for this part

	aheadLogPath := s.getAheadLogPath()
	f, err := os.Open(filepath.Clean(aheadLogPath))

	if os.IsNotExist(err) {
		log.Printf("[WARN] log file %q does not exists", aheadLogPath)
		return nil
	}
	if err != nil {
		return err
	}

	defer func() {
		err := f.Close()
		if err != nil {
			log.Printf("[ERROR] error %v closing log file %q", err, aheadLogPath)
		}
	}()

	reader := bufio.NewReader(f)
	for {
		data, err := reader.ReadBytes(0x0)
		if err != nil {
			for err != io.EOF {
				return nil
			}
			return err
		}
		data = data[:len(data)-1]
		var doc *DocumentComment
		if err = json.Unmarshal(data, doc); err == nil {
			err = s.indexDoc(doc)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
}

// Flush documents buffer
func (s *bleveEngine) Flush() error {
	flusher := &idxFlusher{make(chan error)}

	s.queueLock.Lock()
	s.docQueue.PushBack(flusher)
	s.queueLock.Unlock()

	s.queueNotifier <- true

	return <-flusher.notifier
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

func textMapping(analyzer string, doStore bool) *mapping.FieldMapping {
	textFieldMapping := bleve.NewTextFieldMapping()
	textFieldMapping.Store = doStore
	textFieldMapping.Analyzer = analyzer
	return textFieldMapping
}

func commentDocumentMapping(textAnalyzer string) *mapping.DocumentMapping {
	commentMapping := bleve.NewDocumentMapping()

	commentMapping.AddFieldMappingsAt("text", textMapping(textAnalyzer, false))
	commentMapping.AddFieldMappingsAt("username", textMapping("keyword_lower", true))
	commentMapping.AddFieldMappingsAt(urlFieldName, textMapping("keyword_lower", true))
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

	bReq.Fields = append(bReq.Fields, urlFieldName)

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
