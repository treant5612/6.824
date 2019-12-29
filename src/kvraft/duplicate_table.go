package kvraft

import (
	"fmt"
	"sync"
	"time"
)

var (
	ErrStale   = fmt.Errorf("stale operation")
	ErrTimeout = fmt.Errorf("timeout")
	TimeOut    = time.Second / 2
)

func newDupTable() *DupTable {
	dp := &DupTable{table: make(map[int64]*Result)}
	dp.cond = sync.NewCond(&dp.mu)
	go func() {
		for range time.Tick(10 * time.Millisecond) {
			dp.cond.Broadcast()
		}
	}()
	return dp
}
func restoreDupTable(table map[int64]*Result) *DupTable {
	dp := &DupTable{table: table}
	dp.cond = sync.NewCond(&dp.mu)
	return dp
}

func (t *DupTable) restoreDupTable(table map[int64]*Result) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.table = table
}

type DupTable struct {
	mu    sync.RWMutex
	cond  *sync.Cond
	table map[int64]*Result
}

func (t *DupTable) opStale(op Op) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if r := t.table[op.Cid]; r != nil {
		if op.Seq <= r.Seq {
			return true
		}
	}
	return false
}

func (t *DupTable) setResult(cid int64, result *Result) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.table[cid] = result
	t.cond.Broadcast()
}

func (t *DupTable) getResult(cid int64, seq int64) (result *Result, err error) {
	t.mu.RLock()
	r := t.table[cid]
	t.mu.RUnlock()

	if r != nil {
		switch {
		case r.Seq == seq:
			return r, nil
		case r.Seq > seq:
			return nil, ErrStale
		}
	}

	timeout := time.After(TimeOut)
	ch := make(chan *Result, 1)
	go t.waitResult(cid, seq, ch)

	select {
	case r = <-ch:
		return r, nil
	case <-timeout:
		return nil, ErrTimeout
	}
}

func (t *DupTable) waitResult(cid int64, seq int64, ch chan *Result) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for {
		r := t.table[cid]
		if r != nil && r.Seq == seq {
			ch <- r
			return
		} else if r != nil && r.Seq > seq {
			return
		}
		t.cond.Wait()
	}
}
