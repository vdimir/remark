package internal

import types "github.com/umputun/remark42/backend/app/store/search/types"

type indexerBatch interface {
	Index(id string, data *types.DocumentComment) error
}

type searchEngine interface {
	NewBatch() indexerBatch                               // create new batch
	Batch(batch indexerBatch) error                       // index batch
	Search(req *types.Request) (*types.ResultPage, error) // perform search request
	Delete(id string) error                               // delete document from index
	Close() error                                         // close engine
}

type indexer interface {
	IndexDocument(doc *types.DocumentComment) error
}
