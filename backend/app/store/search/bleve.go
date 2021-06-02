package search

import (
	"fmt"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/pkg/errors"

	store "github.com/umputun/remark42/backend/app/store"

	log "github.com/go-pkgz/lgr"

	bleveCustom "github.com/blevesearch/bleve/analysis/analyzer/custom"
	bleveStandard "github.com/blevesearch/bleve/analysis/analyzer/standard"
	bleveEn "github.com/blevesearch/bleve/analysis/lang/en"
	bleveRu "github.com/blevesearch/bleve/analysis/lang/ru"
	"github.com/blevesearch/bleve/analysis/token/lowercase"
	bleveSingle "github.com/blevesearch/bleve/analysis/tokenizer/single"
	"github.com/blevesearch/bleve/mapping"
	// log "github.com/go-pkgz/lgr"
	// "github.com/blevesearch/bleve"
	// bleveCustom "github.com/blevesearch/bleve/analysis/analyzer/custom"
	// bleveStandard "github.com/blevesearch/bleve/analysis/analyzer/standard"
	// bleveEn "github.com/blevesearch/bleve/analysis/lang/en"
	// bleveRu "github.com/blevesearch/bleve/analysis/lang/ru"
	// bleveSingle "github.com/blevesearch/bleve/analysis/tokenizer/single"
	// "github.com/blevesearch/bleve/analysis/token/lowercase"
	// "github.com/blevesearch/bleve/mapping"
	// "github.com/pkg/errors"
	// types "github.com/umputun/remark42/backend/app/store/search/types"
)

const urlFieldName = "url"
const textFieldName = "text"

// Available text analyzers.
// Bleve supports a bit more languages that may be added,
// see https://github.com/blevesearch/bleve/tree/master/analysis/lang
var analyzerMapping = map[string]string{
	"standard": bleveStandard.Name,
	"english":  bleveEn.AnalyzerName,
	"russian":  bleveRu.AnalyzerName,
}

type bleveEngine struct {
	index bleve.Index
}

// DocumentComment is document that describes comment stored in index
// Bridge between store.Comment and Bleve index
type DocumentComment struct {
	ID        string    `json:"id"`
	SiteID    string    `json:"site"`
	URL       string    `json:"url"`
	Text      string    `json:"text"`
	Timestamp time.Time `json:"timestamp"`
	UserName  string    `json:"username"`
}

const commentDocType = "docComment"

// Type implements bleve.Classifier
func (d DocumentComment) Type() string {
	return commentDocType
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

func (b *bleveEngine) Index(comment *store.Comment) error {
	return b.index.Index(comment.ID, DocFromComment(comment))
}

// convertBleveSerp converts search result
// from bleve internal representation to ResultPage that would passed to user
func convertBleveSerp(bleveResult *bleve.SearchResult) *ResultPage {
	result := ResultPage{
		Total:     bleveResult.Total,
		Documents: make([]ResultDoc, 0, len(bleveResult.Hits)),
	}
	for _, r := range bleveResult.Hits {
		url, hasURL := r.Fields[urlFieldName].(string)
		if !hasURL {
			log.Fatalf("cannot find %q in %v", urlFieldName, r.Fields)
		}

		d := ResultDoc{
			ID:      r.ID,
			Matches: []TokenMatch{},
			PostURL: url,
		}

		if highlight, has := r.Locations[textFieldName]; has {
			for _, locs := range highlight {
				for _, loc := range locs {
					d.Matches = append(d.Matches, TokenMatch{
						Start: loc.Start,
						End:   loc.End,
					})
				}
			}
		}

		result.Documents = append(result.Documents, d)
	}
	return &result
}

func textMapping(analyzer string, doStore bool) *mapping.FieldMapping {
	textFieldMapping := bleve.NewTextFieldMapping()
	textFieldMapping.Store = doStore
	textFieldMapping.Analyzer = analyzer
	textFieldMapping.IncludeTermVectors = true
	return textFieldMapping
}

func commentDocumentMapping(textAnalyzer string) *mapping.DocumentMapping {
	commentMapping := bleve.NewDocumentMapping()

	commentMapping.AddFieldMappingsAt(textFieldName, textMapping(textAnalyzer, false))
	commentMapping.AddFieldMappingsAt("username", textMapping("keyword_lower", true))
	commentMapping.AddFieldMappingsAt(urlFieldName, textMapping("keyword_lower", true))
	return commentMapping
}

// Search performs search request
func (b *bleveEngine) Search(req *Request) (*ResultPage, error) {
	bQuery := bleve.NewQueryStringQuery(req.Query)
	bReq := bleve.NewSearchRequestOptions(bQuery, req.Limit, req.From, false)

	if validateSortField(req.SortBy, "timestamp") {
		bReq.SortBy([]string{req.SortBy})
	} else if req.SortBy != "" {
		log.Printf("[WARN] unknown sort field %q", req.SortBy)
	}

	bReq.Fields = append(bReq.Fields, urlFieldName)
	bReq.Highlight = bleve.NewHighlight()
	bReq.Highlight.AddField(textFieldName)

	serp, err := b.index.Search(bReq)
	if err != nil {
		return nil, errors.Wrap(err, "bleve search error")
	}
	log.Printf("[INFO] found %d documents for query %q in %s",
		serp.Total, req.Query, serp.Took.String())

	result := convertBleveSerp(serp)
	return result, nil
}

// Delete document from index
func (b *bleveEngine) Delete(id string) error {
	return b.index.Delete(id)
}

// Close indexer
func (b *bleveEngine) Close() error {
	return b.index.Close()
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
	indexMapping.AddDocumentMapping(DocumentComment{}.Type(), commentDocumentMapping(textAnalyzer))

	return indexMapping
}
