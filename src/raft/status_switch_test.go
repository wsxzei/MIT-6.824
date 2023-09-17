package raft

import (
	"6.824/labrpc"
	. "github.com/bytedance/mockey"
	. "github.com/smartystreets/goconvey/convey"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCandidate(t *testing.T) {
	// 注: 不要使用convey.Convey, mock的函数不会使用
	// 使用 monkey.PatchConvey, 会重置mock函数
	PatchConvey("Test Candidate", t, func() {
		rf := &Raft{
			me: 0,
			peers: []*labrpc.ClientEnd{
				{}, {}, {}, {}, {},
			}, // 5 个成员, 1 和 3 投赞成票, 2 和 4 反对
			currentTerm:         0,
			status:              NodeStateEnum_Follower,
			resetSelectionTimer: NewBufferChan(make(chan int), make(chan int)),
			resetLeader:         NewBufferChan(make(chan int), make(chan int)),
			resetCandidate:      NewBufferChan(make(chan int), make(chan int)),
		}
		go rf.resetSelectionTimer.Run()
		go rf.resetCandidate.Run()
		go rf.resetLeader.Run()

		PatchConvey("测试选举成功", func() {
			Mock((*Raft).sendRequestVote).To(func(rf *Raft, server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
				reply.Term = args.Term
				if server%2 == 1 {
					// 1, 3
					reply.Voted = true
				} else {
					reply.Voted = false
				}
				return true
			}).Build()

			Mock((*Raft).leader).To(func(rf *Raft, expectTerm int) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				rf.status = NodeStateEnum_Leader
			}).Build()
			rf.candidate()

			time.Sleep(10 * time.Millisecond)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			So(rf.status, ShouldEqual, NodeStateEnum_Leader)
			So(rf.currentTerm, ShouldEqual, 1)

			rf.Kill()
		})

		PatchConvey("测试选举超时, 再次开始选举成功", func() {
			Mock((*Raft).sendRequestVote).To(func(rf *Raft, server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
				reply.Term = args.Term
				if args.Term == 1 {
					// 第一次选举投反对票
					reply.Voted = false
				} else if args.Term == 2 {
					// 第二次选举赞成
					reply.Voted = true
				}
				return true
			}).Build()

			Mock((*Raft).leader).To(func(rf *Raft, expectTerm int) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				rf.status = NodeStateEnum_Leader
			}).Build()

			rf.candidate()

			time.Sleep(2 * time.Millisecond)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			So(rf.status, ShouldEqual, NodeStateEnum_Leader)
			So(rf.currentTerm, ShouldEqual, 2) // 第一任期超时, 第二任期才选举成功

			rf.Kill()
		})

		PatchConvey("测试选举过程中, 其他成员成为Leader", func() {
			Mock((*Raft).leader).To(func(rf *Raft, expectTerm int) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				rf.status = NodeStateEnum_Leader
			}).Build()

			Mock((*Raft).sendRequestVote).To(func(rf *Raft, server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
				reply.Term = args.Term
				reply.Voted = false
				return true
			}).Build()

			go func() {
				time.Sleep(1 * time.Second) // 最多超时 2 次, 任期达到 3
				rf.resetCandidate.Send(3)
			}()
			rf.candidate()

			time.Sleep(2 * time.Millisecond)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			// rf.candidate 能正常退出, 且状态仍然为 Candidate
			So(rf.status, ShouldEqual, NodeStateEnum_Candidate)
			So(rf.currentTerm, ShouldBeLessThanOrEqualTo, 3)
		})
	})
}

func TestFollower(t *testing.T) {

	PatchConvey("Test Follower", t, func() {
		rf := &Raft{
			me:                  0,
			peers:               []*labrpc.ClientEnd{},
			currentTerm:         1,
			status:              NodeStateEnum_Candidate,
			resetSelectionTimer: NewBufferChan(make(chan int), make(chan int)),
			resetLeader:         NewBufferChan(make(chan int), make(chan int)),
			resetCandidate:      NewBufferChan(make(chan int), make(chan int)),
		}
		go rf.resetSelectionTimer.Run()
		go rf.resetCandidate.Run()
		go rf.resetLeader.Run()

		PatchConvey("Candidate To Follower", func() {
			i := int32(0)
			rf.toFollower = sync.NewCond(&rf.mu)
			go func() {
				rf.mu.Lock()
				rf.toFollower.Wait()
				rf.mu.Unlock()
				atomic.AddInt32(&i, 1)
			}()

			time.Sleep(100 * time.Millisecond)
			rf.mu.Lock()
			rf.follower(2, nil)
			rf.mu.Unlock()

			time.Sleep(100 * time.Millisecond)
			So(i, ShouldEqual, 1) // 测试等待 toFollower的 Goroutine 被唤醒

			eventTerm, ok := rf.resetCandidate.Receive()
			So(ok, ShouldEqual, true)
			So(eventTerm, ShouldEqual, 1)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			So(rf.status, ShouldEqual, NodeStateEnum_Follower)
			So(rf.currentTerm, ShouldEqual, 2)

			rf.Kill()
		})
	})
}

func TestLeader(t *testing.T) {
	PatchConvey("Test leader", t, func() {
		rf := &Raft{
			me: 0,
			peers: []*labrpc.ClientEnd{
				{}, {}, {},
			},
			currentTerm:         1,
			status:              NodeStateEnum_Candidate,
			resetSelectionTimer: NewBufferChan(make(chan int), make(chan int)),
			resetLeader:         NewBufferChan(make(chan int), make(chan int)),
			resetCandidate:      NewBufferChan(make(chan int), make(chan int)),
		}
		go rf.resetSelectionTimer.Run()
		go rf.resetCandidate.Run()
		go rf.resetLeader.Run()

		PatchConvey("测试Leader收到更高任期的响应, 重新切换为Follower", func() {
			rf.toFollower = sync.NewCond(&rf.mu)
			Mock((*Raft).sendAppendEntries).To(func(rf *Raft, server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
				reply.Term = 2
				return true
			}).Build()
			rf.leader(1)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 预期切换为任期 2 的 follower
			So(rf.currentTerm, ShouldEqual, 2)
			So(rf.status, ShouldEqual, NodeStateEnum_Follower)
			rf.Kill()
		})

		PatchConvey("测试心跳RPC次数", func() {
			// 测试一段时间内, heartbeat 方法调用的次数
			sleepTime := time.Second
			cnt := int32(0)

			Mock((*Raft).heartbeat).To(func(rf *Raft, server int, argTerm int) {
				if server == 1 {
					atomic.AddInt32(&cnt, 1)
				}
			}).Build()

			go rf.leader(1)

			time.Sleep(sleepTime)
			rf.resetLeader.Send(1)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			So(rf.currentTerm, ShouldEqual, 1)
			So(rf.status, ShouldEqual, NodeStateEnum_Leader)
			// 1000 / 150 = 6, 总共会有 7 轮心跳
			So(cnt, ShouldEqual, time.Second/HeartbeatPeriod+1)

			rf.Kill()
		})

	})
}
