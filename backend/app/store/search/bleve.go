package search

import (
	"fmt"
	"os"
	"time"

	log "github.com/go-pkgz/lgr"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/mapping"
	"github.com/pkg/errors"
	"github.com/umputun/remark42/backend/app/store"
	"github.com/umputun/remark42/backend/app/store/engine"

	"github.com/blevesearch/bleve/analysis/analyzer/keyword"
	bleveStandard "github.com/blevesearch/bleve/analysis/analyzer/standard"
	bleveEn "github.com/blevesearch/bleve/analysis/lang/en"
	bleveRu "github.com/blevesearch/bleve/analysis/lang/ru"
)

// BleveService provides search
type BleveService struct {
	index bleve.Index
}

const commentDocType = "docComment"
const urlFiledName = "url"

// Avaliable text analyzers.
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

// NewBleveService returns new BleveService instance
func NewBleveService(indexPath string, analyzer string) (Searcher, error) {
	if _, ok := analyzerMapping[analyzer]; !ok {
		analyzers := make([]string, 0, len(analyzerMapping))
		for k := range analyzerMapping {
			analyzers = append(analyzers, k)
		}
		return nil, errors.Errorf("Unknown analyzer: %q. Avaliable analyzers for bleve: %v", analyzer, analyzers)
	}
	var index bleve.Index
	var err error

	if _, errOpen := os.Stat(indexPath); os.IsNotExist(errOpen) {
		log.Printf("[INFO] creating new search index %s", indexPath)
		index, err = bleve.New(indexPath, createIndexMapping(analyzerMapping[analyzer]))
	} else if errOpen == nil {
		log.Printf("[INFO] opening existing search index %s", indexPath)
		index, err = bleve.Open(indexPath)
	} else {
		err = errOpen
	}

	if err != nil {
		return nil, errors.Wrap(err, "cannot create/open index")
	}

	return &BleveService{
		index: index,
	}, nil
}

// IndexDocument adds or updates document to search index
func (s *BleveService) IndexDocument(commentID string, comment *store.Comment) error {
	doc := DocFromComment(comment)
	log.Printf("[INFO] index document %s", commentID)
	return s.index.Index(commentID, doc)
}

func createIndexMapping(textAnalyzer string) mapping.IndexMapping {
	indexMapping := bleve.NewIndexMapping()

	indexMapping.AddDocumentMapping(commentDocType, commentDocumentMapping(textAnalyzer))

	return indexMapping
}

func textMapping(analyzer string, store bool) *mapping.FieldMapping {
	textFieldMapping := bleve.NewTextFieldMapping()
	textFieldMapping.Store = store
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

func (s *BleveService) Search(req *Request) (*ResultPage, error) {
	log.Printf("[INFO] searching %v", req)

	bQuery := bleve.NewQueryStringQuery(req.Query)
	bReq := bleve.NewSearchRequest(bQuery)
	bReq.SortBy([]string{"-timestamp"})

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
		url, hasUrl := r.Fields[urlFiledName].(string)
		if !hasUrl {
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

// WrapEngine decorates engine
func (s *BleveService) WrapEngine(e engine.Interface) engine.Interface {
	return &EngineDecorator{
		Interface: e,
		searcher:  s,
	}
}
