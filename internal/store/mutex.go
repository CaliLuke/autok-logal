package store

import (
	"context"
	"sync"
)

// contextMutex serializes access to the single writer without trapping canceled
// exports or status requests behind maintenance. Its zero value is usable.
type contextMutex struct {
	once   sync.Once
	permit chan struct{}
}

func (m *contextMutex) LockContext(ctx context.Context) error {
	m.once.Do(func() { m.permit = make(chan struct{}, 1) })
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case m.permit <- struct{}{}:
		if err := ctx.Err(); err != nil {
			m.Unlock()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *contextMutex) Lock()   { _ = m.LockContext(context.Background()) }
func (m *contextMutex) Unlock() { <-m.permit }
