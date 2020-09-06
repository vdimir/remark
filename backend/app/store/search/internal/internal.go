package internal

import service "github.com/umputun/remark42/backend/app/store/search/service"

type indexerBatch interface {
	Index(id string, data *service.DocumentComment) error
}

type searchEngine interface {
	NewBatch() indexerBatch                                   // create new batch
	Batch(batch indexerBatch) error                           // index batch
	Search(req *service.Request) (*service.ResultPage, error) // perform search request
	Delete(id string) error                                   // delete document from index
	Close() error                                             // close engine
}

type indexer interface {
	IndexDocument(doc *service.DocumentComment) error
}
