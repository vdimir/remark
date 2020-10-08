package search

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
	"github.com/umputun/remark42/backend/app/store"
	"github.com/umputun/remark42/backend/app/store/engine"
)

func TestDecorator(t *testing.T) {
	engineMock := &engine.MockInterface{}
	searchMock := &MockService{}
	wEngine := WrapEngine(engineMock, searchMock)

	newComment := func(i int, text string) store.Comment {
		uid := fmt.Sprintf("user%d", i%7)
		return store.Comment{
			ID:      fmt.Sprintf("comment%d", i),
			Text:    fmt.Sprintf("%s %d", text, i*100),
			Locator: store.Locator{SiteID: "remark", URL: fmt.Sprintf("https://radio-t.com/blah%d", i%4)},
			User:    store.User{Name: uid, ID: uid}}
	}

	commentsCount := 10
	searchMock.On("IndexDocument", mock.Anything).Times(commentsCount).Return(nil)
	for i := 1; i < commentsCount+1; i++ {
		comment := newComment(i, "test test")
		engineMock.On("Create", comment).Return(comment.ID, nil)
		_, err := wEngine.Create(comment)
		assert.NoError(t, err)
	}
	searchMock.AssertNumberOfCalls(t, "IndexDocument", commentsCount)

	updatesCount := 5
	searchMock.On("IndexDocument", mock.Anything).Times(updatesCount).Return(nil)
	for i := 1; i < updatesCount+1; i++ {
		comment := newComment(i, "upd test")
		engineMock.On("Update", comment).Return(nil)
		err := wEngine.Update(comment)
		assert.NoError(t, err)
	}
	searchMock.AssertNumberOfCalls(t, "IndexDocument", commentsCount+updatesCount)
}
