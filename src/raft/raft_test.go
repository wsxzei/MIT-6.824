package raft

import (
	"6.824/labrpc"
	. "github.com/bytedance/mockey"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestRequestVote(t *testing.T) {
	PatchConvey("Test RequestVote", t, func() {

		rf := &Raft{
			me:          0,
			peers:       []*labrpc.ClientEnd{},
			currentTerm: 3,
			status:      NodeStateEnum_Follower,
			log: []*LogEntry{
				{Term: 1}, {Term: 3},
			},
			resetSelectionTimer: NewBufferChan(make(chan int), make(chan int)),
			resetLeader:         NewBufferChan(make(chan int), make(chan int)),
			resetCandidate:      NewBufferChan(make(chan int), make(chan int)),
		}
		go rf.resetSelectionTimer.Run()
		go rf.resetCandidate.Run()
		go rf.resetLeader.Run()

		// 发起者任期更大, 日志最新, 投赞成票
		PatchConvey("赞成票, 接收者切换Follower", func() {
			args := &RequestVoteArgs{
				Term:         4,
				CandidateId:  1,
				LastLogIndex: 2,
				LastLogTerm:  3,
			}

			reply := &RequestVoteReply{}
			rf.RequestVote(args, reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 当前状态为 Follower, 任期为 4
			So(rf.status, ShouldEqual, NodeStateEnum_Follower)
			So(rf.currentTerm, ShouldEqual, 4)
			// voteFor 等于 1
			So(rf.votedFor, ShouldNotBeNil)
			So(*rf.votedFor, ShouldEqual, 1)

			So(reply.Voted, ShouldEqual, true)
			// resetSelectionTimer 中能接收到 重置 Follower 定时器事件
			time.Sleep(2 * time.Millisecond)
			eventTerm, _ := rf.resetSelectionTimer.Receive()
			So(eventTerm, ShouldEqual, 3)
		})

		// 发起者任期更大, 日志不是最新, 投反对票
		PatchConvey("反对票, 接收者切换Follower", func() {
			args := &RequestVoteArgs{
				Term:         4,
				CandidateId:  1,
				LastLogIndex: 2,
				LastLogTerm:  2,
			}
			reply := &RequestVoteReply{}
			rf.RequestVote(args, reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			So(rf.status, ShouldEqual, NodeStateEnum_Follower)
			So(rf.currentTerm, ShouldEqual, 4)
			So(rf.votedFor, ShouldBeNil)
			So(reply.Voted, ShouldEqual, false)

			rf.Kill()
		})

		// 发起者任期相同, 日志最新, 接收者已经投过接收者的票, 再次投赞成票
		PatchConvey("赞成票, 不切换状态", func() {
			rf.votedFor = NewInt(1)
			args := &RequestVoteArgs{
				Term:         3,
				CandidateId:  1,
				LastLogIndex: 1,
				LastLogTerm:  3,
			}

			reply := &RequestVoteReply{}
			rf.RequestVote(args, reply)

			So(rf.status, ShouldEqual, NodeStateEnum_Follower)
			So(rf.currentTerm, ShouldEqual, 3)

			So(rf.votedFor, ShouldNotBeNil)
			So(*rf.votedFor, ShouldEqual, 1)

			So(reply.Voted, ShouldEqual, true)

			time.Sleep(2 * time.Millisecond)
			eventTerm, _ := rf.resetSelectionTimer.Receive()
			So(eventTerm, ShouldEqual, 3)
		})

	})
}

func TestAppendEntries(t *testing.T) {

}

func TestTicker(t *testing.T) {

}
