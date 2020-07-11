package search

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/umputun/remark42/backend/app/store"
	"github.com/umputun/remark42/backend/app/store/engine"
)

// elastic implements Service directly (not searchEngine)
// because it has own mechanisms for batching
// and do not reuire additional housekeeping
type elastic struct {
	client       *elasticsearch.Client
	bulkIndexers map[string]esutil.BulkIndexer
	ctx          context.Context
	cancel       context.CancelFunc
}

type elacticQuery struct {
	Query struct {
		Match struct {
			Text string `json:"text"`
		} `json:"match"`
	} `json:"start"`
	Size int `json:"total"`
	From int `json:"from"`
}

type elacticResponse struct {
	Took int
	Hits struct {
		Total struct {
			Value int
		}
		Hits []struct {
			ID         string          `json:"_id"`
			Source     json.RawMessage `json:"_source"`
			Highlights json.RawMessage `json:"highlight"`
			Sort       []interface{}   `json:"sort"`
		}
	}
}

func newElasticService(params SearcherParams) (Service, error) {
	if params.Endpoint == "" {
		return nil, errors.Errorf("elasticsearch parameters are not set")
	}

	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{params.Endpoint},
	})
	if err != nil {
		return nil, err
	}

	bulkIndexers := map[string]esutil.BulkIndexer{}
	for _, siteID := range params.Sites {
		bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
			Index:  siteID,
			Client: client,
		})
		if err != nil {
			return nil, err
		}
		bulkIndexers[siteID] = bi
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &elastic{
		client:       client,
		bulkIndexers: bulkIndexers,
		ctx:          ctx,
		cancel:       cancel,
	}, nil
}

func (e *elastic) IndexDocument(commentID string, comment *store.Comment) error {
	doc := DocFromComment(comment)

	data, err := json.Marshal(doc)
	if err != nil {
		return errors.Wrapf(err, "cannot encode document %s: %s", doc.ID, err)
	}
	siteID := comment.Locator.SiteID

	if bi, has := e.bulkIndexers[siteID]; has {
		err = bi.Add(
			e.ctx,
			esutil.BulkIndexerItem{
				Action:     "index",
				DocumentID: doc.ID,
				Body:       bytes.NewReader(data),
				OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
					if err != nil {
						log.Printf("[ERROR] failed to index document: %s", err)
					} else {
						log.Printf("[ERROR]: %s: %s", res.Error.Type, res.Error.Reason)
					}
				},
			},
		)
	} else {
		err = errors.Errorf("index for site %s does not found", siteID)
	}

	if err != nil {
		return errors.Wrap(err, "failed to add document to batch")
	}

	return nil
}

func (e *elastic) buildQuery(req *Request) io.Reader {
	var buf bytes.Buffer
	query := elacticQuery{
		Size: req.Limit,
		From: req.From,
	}
	query.Query.Match.Text = req.Query

	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		log.Fatalf("Error encoding query: %s", err)
	}

	return &buf
}

func (e *elastic) Search(req *Request) (*ResultPage, error) {
	resp, err := e.client.Search(
		e.client.Search.WithIndex(req.SiteID),
		e.client.Search.WithBody(e.buildQuery(req)),
	)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.IsError() {
		var e struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
			return nil, errors.Wrap(err, "error parsing the response body")
		}
		return nil, errors.Errorf("search error %s: %s", e.Type, e.Reason)
	}
	var r elacticResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, errors.Wrap(err, "error parsing the response body")
	}
	serp := &ResultPage{
		Total:     uint64(r.Hits.Total.Value),
		Documents: make([]ResultDoc, len(r.Hits.Hits)),
	}
	// for _, v := r.Hits.Hits {
	// }
	return serp, nil
}

func (e *elastic) Init(ctx context.Context, eng engine.Interface) error {

	return nil
}

func (e *elastic) Delete(siteID, commentID string) error {
	var err error
	if bi, has := e.bulkIndexers[siteID]; has {
		err = bi.Add(
			e.ctx,
			esutil.BulkIndexerItem{
				Action:     "delete",
				DocumentID: commentID,
				OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
					if err != nil {
						log.Printf("[ERROR] failed to delete document: %s", err)
					} else {
						log.Printf("[ERROR]: %s: %s", res.Error.Type, res.Error.Reason)
					}
				},
			},
		)
	} else {
		err = errors.Errorf("index for site %s does not found", siteID)
	}
	return err
}

func (e *elastic) Flush(siteID string) error {
	// TODO(@vdimir)
	return nil
}

func (e *elastic) Close() error {
	e.cancel()

	errs := new(multierror.Error)
	for siteID, bi := range e.bulkIndexers {
		err := bi.Close(context.Background())
		if err != nil {
			errs = multierror.Append(err, errors.Wrapf(err, "cannot close indexer for site %s", siteID))
		}
	}
	return errs.ErrorOrNil()
}

func (e *elastic) Ready() bool {
	return true
}

func (e *elastic) Type() string {
	return "elastic"
}
