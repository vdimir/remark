package search

import (
	log "github.com/go-pkgz/lgr"

	"github.com/umputun/remark42/backend/app/store"
	"github.com/umputun/remark42/backend/app/store/engine"
)

// StoreEngineDecorator proxies requests to store/engine.Interface and index incoming data
type StoreEngineDecorator struct {
	engine.Interface
	searcher Service
}

// WrapEngine decorates engine with StoreEngineDecorator
func WrapEngine(e engine.Interface, s Service) engine.Interface {
	return &StoreEngineDecorator{
		Interface: e,
		searcher:  s,
	}
}

// Create comment and add to index
func (e *StoreEngineDecorator) Create(comment store.Comment) (commentID string, err error) {
	commentID, err = e.Interface.Create(comment)
	if err != nil {
		return commentID, err
	}
	if err = e.searcher.IndexDocument(&comment); err != nil {
		log.Printf("[WARN] failed to add document to index, %v", err)
	}
	return commentID, err
}

// Update comment and index
func (e *StoreEngineDecorator) Update(comment store.Comment) error {
	if err := e.Interface.Update(comment); err != nil {
		return err
	}

	if err := e.searcher.IndexDocument(&comment); err != nil {
		log.Printf("[WARN] failed to update document in index, %v", err)
	}
	return nil
}

// Delete comment from storage and index
func (e *StoreEngineDecorator) Delete(req engine.DeleteRequest) error {
	if err := e.Interface.Delete(req); err != nil {
		return err
	}
	if req.Locator.SiteID == "" || req.CommentID == "" {
		return nil
	}
	return e.searcher.Delete(req.Locator.SiteID, req.CommentID)
}
