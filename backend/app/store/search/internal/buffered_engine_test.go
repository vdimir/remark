package internal

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	types "github.com/umputun/remark42/backend/app/store/search/types"
)

type mockSearchEngine struct {
	DocIndexed []string
}

type mockBatch struct {
	DocIds []string
	parent *mockSearchEngine
}

func (b *mockBatch) Index(id string, data *types.DocumentComment) error {
	b.DocIds = append(b.DocIds, id)
	return nil
}

func (se *mockSearchEngine) NewBatch() indexerBatch {
	return &mockBatch{parent: se}
}

func (se *mockSearchEngine) Batch(batch indexerBatch) error {
	se.DocIndexed = append(se.DocIndexed, batch.(*mockBatch).DocIds...)
	return nil
}

func (se *mockSearchEngine) Search(req *types.Request) (*types.ResultPage, error) {
	return &types.ResultPage{}, nil
}

func (se *mockSearchEngine) Delete(id string) error {
	return nil
}

func (se *mockSearchEngine) Close() error {
	return nil
}

func (se *mockSearchEngine) numDocIndexedPred(n int) func() bool {
	return func() bool {
		return len(se.DocIndexed) == n
	}
}

func newMockEngine(idxPath string) (*bufferedEngine, *mockSearchEngine) {
	mock := &mockSearchEngine{}
	infinite := time.Hour
	eng := &bufferedEngine{
		index:         mock,
		queueNotifier: make(chan bool),
		flushEvery:    infinite,
		flushCount:    3,
		indexPath:     idxPath,
	}
	return eng, mock
}

func TestAheadLog(t *testing.T) {

	idxPath := os.TempDir() + "/search-remark42"
	require.NoError(t, os.MkdirAll(idxPath, 0700))

	defer func() {
		_ = os.RemoveAll(idxPath)
	}()

	checkBackoff := 10 * time.Millisecond
	checkTimeout := 200 * time.Millisecond
	{
		eng, mock := newMockEngine(idxPath)
		hotStart, err := eng.Init(context.TODO())
		assert.NoError(t, err)
		assert.False(t, hotStart)
		eng.Start()

		assert.NoError(t, eng.IndexDocument(&types.DocumentComment{ID: "1"}))
		assert.Len(t, mock.DocIndexed, 0)

		assert.NoError(t, eng.IndexDocument(&types.DocumentComment{ID: "2"}))
		assert.Len(t, mock.DocIndexed, 0)

		assert.NoError(t, eng.IndexDocument(&types.DocumentComment{ID: "3"}))
		assert.Eventually(t, mock.numDocIndexedPred(3), checkTimeout, checkBackoff)

		assert.NoError(t, eng.IndexDocument(&types.DocumentComment{ID: "4"}))
		assert.Len(t, mock.DocIndexed, 3)

		assert.NoError(t, eng.Close())
	}

	// Test hot start
	{
		eng, mock := newMockEngine(idxPath)
		hotStart, err := eng.Init(context.TODO())
		assert.NoError(t, err)
		assert.True(t, hotStart)
		eng.Start()

		// one doc should be read from ahed log
		assert.NoError(t, eng.IndexDocument(&types.DocumentComment{ID: "10"}))
		assert.Len(t, mock.DocIndexed, 0)

		assert.NoError(t, eng.IndexDocument(&types.DocumentComment{ID: "20"}))

		assert.Eventually(t, mock.numDocIndexedPred(3), checkTimeout, checkBackoff)

		assert.NoError(t, eng.Close())
	}
}
