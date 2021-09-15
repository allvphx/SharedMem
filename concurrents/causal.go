package concurrents

import (
	"container/heap"
	"sync"
	"time"
)

type TimeStamp []int

type CausalMsg struct {
	SourceID int
	Key      int
	Value    interface{}
	Ts       TimeStamp
}

type Queue []*CausalMsg

type CausalMemory struct {
	SharedMemory
	mu       *sync.Mutex
	id       int
	LocalMem []interface{}
	ts       []int
	inQueue  Queue
	outQueue Queue
}

func (h *Queue) Len() int {
	return len(*h)
}
func (h *Queue) Less(i, j int) bool {
	res := false
	for t, v := range (*h)[i].Ts {
		if v < (*h)[j].Ts[t] {
			res = true
		} else if v > (*h)[j].Ts[t] {
			return false
		}
	}
	return res
}
func (h *Queue) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}
func (h *Queue) Push(x interface{}) {
	*h = append(*h, x.(*CausalMsg))
}
func (h *Queue) Pop() interface{} {
	x := (*h)[len(*h)-1]
	*h = (*h)[0 : len(*h)-1]
	return x
}
func (h *Queue) Pop_Queue() interface{} {
	x := (*h)[0]
	*h = (*h)[1:]
	return x
}

// Init initialize the local memory with process id, size of memory, and the number of nodes.
func (c *CausalMemory) Init(pID int, size int, memberN int) bool {
	c.id = pID
	c.LocalMem = make([]interface{}, size)
	c.ts = make([]int, memberN)
	c.mu = &sync.Mutex{}
	c.inQueue = make([]*CausalMsg, 0)
	c.outQueue = make([]*CausalMsg, 0)
	heap.Init(&c.inQueue)
	return true
}

// Stopped testify if the memory is stopped.
func (c *CausalMemory) Stopped() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.id == -1
}

// Read local read.
func (c *CausalMemory) Read(key int) interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.LocalMem[key]
}

// Write local write.
func (c *CausalMemory) Write(key int, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ts[c.id]++
	c.LocalMem[key] = value
	c.outQueue.Push(&CausalMsg{
		SourceID: c.id,
		Key:      key,
		Value:    value,
		Ts:       c.ts,
	})
}

func (c *CausalMemory) Broadcast(msg *CausalMsg) {
}

func (c *CausalMemory) sendLoop() {
	for !c.Stopped() {
		time.Sleep(10 * time.Millisecond)
		c.mu.Lock()
		if c.outQueue.Len() > 0 {
			c.Broadcast(c.outQueue.Pop_Queue().(*CausalMsg))
		}
		c.mu.Unlock()
	}
}

func (c *CausalMemory) applyLoop() {
	for !c.Stopped() {
		time.Sleep(10 * time.Millisecond)
		c.mu.Lock()
		if c.inQueue.Len() == 0 {
			c.mu.Unlock()
			continue
		}
		cur := c.inQueue.Pop().(*CausalMsg)
		j := cur.SourceID
		if cur.Ts[j] != c.ts[j]+1 {
			c.inQueue.Push(cur)
			c.mu.Unlock()
			continue
		}
		cmp := true
		for i, _ := range c.ts {
			if c.ts[i] < cur.Ts[i] {
				cmp = false
			}
		}
		if !cmp {
			c.inQueue.Push(cur)
			c.mu.Unlock()
			continue
		}
		c.ts[j] = cur.Ts[j]
		c.LocalMem[cur.Key] = cur.Value
		c.mu.Unlock()
	}
}
