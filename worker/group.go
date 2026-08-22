package worker

import (
	"context"
	"sync"
)

type inflightGroup struct {
	mu        sync.Mutex
	accepting bool
	inflight  int
	drained   chan struct{}
	closeOnce sync.Once
}

func newInflightGroup() *inflightGroup {
	return &inflightGroup{drained: make(chan struct{})}
}

func (g *inflightGroup) open() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.accepting = true
}

func (g *inflightGroup) begin() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.accepting {
		return false
	}
	g.inflight++
	return true
}

func (g *inflightGroup) done() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.inflight == 0 {
		return
	}
	g.inflight--
	g.signalDrainedLocked()
}

func (g *inflightGroup) stopAndWait(ctx context.Context) error {
	g.mu.Lock()
	g.accepting = false
	g.signalDrainedLocked()
	drained := g.drained
	g.mu.Unlock()

	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (g *inflightGroup) signalDrainedLocked() {
	if g.accepting || g.inflight != 0 {
		return
	}
	g.closeOnce.Do(func() {
		close(g.drained)
	})
}
