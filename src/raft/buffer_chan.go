package raft

import (
	"log"
	"sync"
)

type BufferIntChan struct {
	buffer []int
	in     chan int // 数据读取Channel
	out    chan int // 数据发送Channel

	exit chan struct{} // 用于强制退出 Run

	rwLk   sync.RWMutex
	closed bool // 关闭 in
	killed bool
}

func NewBufferChan(in chan int, out chan int) *BufferIntChan {
	if in == nil || out == nil {
		log.Panicf("[NewBufferChan] invaid params\n")
	}

	return &BufferIntChan{
		buffer: make([]int, 0),
		in:     in,
		out:    out,
		exit:   make(chan struct{}, 1),
	}
}

func (r *BufferIntChan) Run() {
	for !r.invalidBufferChan() {
		var (
			sendOut chan<- int
			rcvFrom <-chan int = r.in
			nextInt int
		)

		if len(r.buffer) > 0 {
			sendOut = r.out
			nextInt = r.buffer[0]
		}

		select {
		case sendOut <- nextInt:
			// 若r.buffer中包含元素, sendOut不为nil, send成功后更新r.buffer
			r.buffer = r.buffer[1:]
		case i, ok := <-rcvFrom:
			if !ok {
				// r.in 被关闭
				r.in = nil
				continue
			}
			r.buffer = append(r.buffer, i)
		case <-r.exit:
			break
		}
	}
	close(r.out)
}

func (r *BufferIntChan) invalidBufferChan() bool {
	return r.isKilled() || (r.in == nil && len(r.buffer) == 0)
}

// Close 通过关闭in channel, 在处理完缓冲区内的消息后, 优雅地退出 Run Goroutine
func (r *BufferIntChan) Close() {
	r.rwLk.Lock()
	defer r.rwLk.Unlock()

	if !r.closed {
		r.closed = true
		close(r.in)
	}
}

// Kill 粗暴地退出Run Goroutine
func (r *BufferIntChan) Kill() {
	r.rwLk.Lock()
	defer r.rwLk.Unlock()
	//if !r.closed {
	//	r.closed = true
	//	close(r.in)
	//}
	if !r.killed {
		r.killed = true
		r.exit <- struct{}{}
	}
}

func (r *BufferIntChan) Send(i int) bool {
	r.rwLk.RLock()
	defer r.rwLk.RUnlock()
	if r.closed || r.killed {
		return false
	}

	// 需要上锁, 防止在发送过程中, 被Kill关闭r.in.
	// 对关闭的channel发送数据会panic
	if r != nil && r.in != nil {
		r.in <- i
		return true
	}
	return false
}

func (r *BufferIntChan) Receive() (int, bool) {
	if r != nil && r.out != nil {
		i, ok := <-r.out
		return i, ok
	}
	return 0, false
}

func (r *BufferIntChan) ReceiveAsync() <-chan int {
	if r != nil && r.out != nil {
		return r.out
	}
	return nil
}

func (r *BufferIntChan) isClosed() bool {
	r.rwLk.RLock()
	defer r.rwLk.RUnlock()

	return r.closed
}

func (r *BufferIntChan) isKilled() bool {
	r.rwLk.RLock()
	defer r.rwLk.RUnlock()
	return r.killed
}
