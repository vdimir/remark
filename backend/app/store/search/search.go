// Package search provides full-test search functionality for site
package search

import (
	"github.com/blevesearch/bleve"
)

// Service provides search
type Service struct {
	index bleve.Index
}
