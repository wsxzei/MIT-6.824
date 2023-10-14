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

func TestUpToDate(t *testing.T) {
	Convey("Test log Uptodate", t, func() {
		// case1: 任期更大, 但索引更小
		Convey("Term is greater, index is less", func() {
			rf := &Raft{
				log: []*LogEntry{
					{
						Term: 1,
					},
					{
						Term: 3,
					},
				},
			}
			lastLogTerm, lastLogIndex := 4, 0
			isUptoDate := rf.upToDate(lastLogIndex, lastLogTerm)
			So(isUptoDate, ShouldEqual, true)
		})
		// case2: 任期更小, 但索引更大
		Convey("Term is less, index is greater", func() {
			rf := &Raft{
				log: []*LogEntry{
					{
						Term: 1,
					},
					{
						Term: 3,
					},
				},
			}
			lastLogTerm, lastLogIndex := 2, 4
			isUptoDate := rf.upToDate(lastLogIndex, lastLogTerm)
			So(isUptoDate, ShouldEqual, false)
		})

		// case3: 任期相同, 索引更大
		Convey("Term is equal, index is greater", func() {
			rf := &Raft{
				log: []*LogEntry{
					{
						Term: 1,
					},
					{
						Term: 3,
					},
				},
			}
			lastLogTerm, lastLogIndex := 3, 3
			isUptoDate := rf.upToDate(lastLogIndex, lastLogTerm)
			So(isUptoDate, ShouldEqual, true)
		})

		// 任期相同, 索引相同
		Convey("Term is equal, index is equal", func() {
			rf := &Raft{
				log: []*LogEntry{
					{
						Term: 1,
					},
					{
						Term: 3,
					},
				},
			}
			lastLogTerm, lastLogIndex := 3, 1
			isUptoDate := rf.upToDate(lastLogIndex, lastLogTerm)
			So(isUptoDate, ShouldEqual, true)
		})

		// 任期相同, 索引更小
		Convey("Term is equal, index is less", func() {
			rf := &Raft{
				log: []*LogEntry{
					{
						Term: 1,
					},
					{
						Term: 3,
					},
					{
						Term: 3,
					},
				},
			}
			lastLogTerm, lastLogIndex := 3, 1
			isUptoDate := rf.upToDate(lastLogIndex, lastLogTerm)
			So(isUptoDate, ShouldEqual, false)
		})

	})
}

func TestApplier(t *testing.T) {
	PatchConvey("Test Applier", t, func() {
		applyCh := make(chan ApplyMsg)
		rf := &Raft{
			me:          0,
			peers:       []*labrpc.ClientEnd{},
			currentTerm: 3,
			status:      NodeStateEnum_Follower,
			log: []*LogEntry{
				{Term: 0}, {Term: 1, Command: 2}, {Term: 1, Command: 2}, {Term: 3, Command: 3},
			},
			applyCh:             applyCh,
			commitIdx:           4,
			lastApplied:         4,
			snapshotIdx:         3,
			resetSelectionTimer: NewBufferChan(make(chan int), make(chan int)),
			resetCandidate:      NewBufferChan(make(chan int), make(chan int)),
		}
		rf.logCommit = sync.NewCond(&rf.mu)
		go rf.resetSelectionTimer.Run()
		go rf.resetCandidate.Run()
		go rf.applier()

		go func() {
			time.Sleep(10 * time.Millisecond)
			rf.mu.Lock()
			rf.commitIdx = 6
			rf.logCommit.Broadcast()
			rf.mu.Unlock()
		}()

		applyMsgList := []ApplyMsg{}
		for len(applyMsgList) != 2 {
			select {
			case msg := <-rf.applyCh:
				applyMsgList = append(applyMsgList, msg)
			}
		}
		So(applyMsgList, ShouldResemble, []ApplyMsg{
			{
				Command:      2,
				CommandValid: true,
				CommandIndex: 5,
			},
			{
				Command:      3,
				CommandValid: true,
				CommandIndex: 6,
			},
		})
	})
}

// go test -gcflags="all=-l -N" --race -run TestSyncEntriesToFollower
func TestSyncEntriesToFollower(t *testing.T) {
	PatchConvey("Test Sync", t, func() {
		rf := &Raft{
			me:          0,
			peers:       []*labrpc.ClientEnd{{}, {}, {}},
			currentTerm: 3,
			status:      NodeStateEnum_Leader,
			log: []*LogEntry{
				{Term: 1}, {Term: 1, Command: 2}, {Term: 1, Command: 2}, {Term: 3, Command: 3},
			},
			snapshotIdx: 3,
		}
		rf.logCommit = sync.NewCond(&rf.mu)

		PatchConvey("sendAppendEntries Success, pass consistency check", func() {
			rf.nextIdx = []int{7, 5, 7} // nextIdx[1] = 5
			rf.matchIdx = []int{6, 4, 6}
			expectArgs := &AppendEntriesArgs{
				Term:        3,
				LeaderId:    0,
				PrevLogIdx:  4,
				PrevLogTerm: 1,
				Entries:     []*LogEntry{{Term: 1, Command: 2}, {Term: 3, Command: 3}},
			}
			var actualArgs *AppendEntriesArgs
			Mock((*Raft).sendAppendEntries).To(func(rf *Raft, server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
				actualArgs = args
				reply.Success = true
				reply.Term = 3
				return true
			}).Build()

			rf.mu.Lock()
			rf.syncEntriesToFollower(1, 3) // 持有锁调用syncEntriesToFollower, 返回时仍然持有锁

			So(actualArgs, ShouldResemble, expectArgs)

			So(rf.status, ShouldEqual, NodeStateEnum_Leader)
			So(rf.currentTerm, ShouldEqual, 3)

			// 测试 nextIdx 和 matchIdx 更新
			So(rf.nextIdx, ShouldResemble, []int{7, 7, 7})
			So(rf.matchIdx, ShouldResemble, []int{6, 6, 6})
			rf.mu.Unlock()
		})

		PatchConvey("Failed consistence check in the first time", func() {
			// Index 	4		5		6
			// Leader 	1		1		3
			// Follower 1		1		1

			rf.nextIdx = []int{7, 7, 7}
			rf.matchIdx = []int{6, 3, 6}
			expectArgs_1 := &AppendEntriesArgs{
				Term:        3,
				LeaderId:    0,
				PrevLogIdx:  6,
				PrevLogTerm: 3,
				Entries:     []*LogEntry{},
			}
			expectArgs_2 := &AppendEntriesArgs{
				Term:        3,
				LeaderId:    0,
				PrevLogIdx:  5,
				PrevLogTerm: 1,
				Entries:     []*LogEntry{{Term: 3, Command: 3}},
			}

			PatchConvey("conflictIndex isn't less than snapshotIdx", func() {
				var actualArgs_1, actualArgs_2 *AppendEntriesArgs

				callCnt := 0
				Mock((*Raft).sendAppendEntries).To(func(rf *Raft, server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
					reply.Term = 3
					// 第一次调用 Success 失败, prevLogIndex=6, prevLogTerm=3 与 Follower 本地日志不匹配
					if callCnt == 0 {
						actualArgs_1 = args
						callCnt++
						reply.Success = false
						reply.ConflictIdx = 4
						reply.ConflictTerm = NewInt(1)
						return true
					}

					if callCnt == 1 {
						actualArgs_2 = args
						reply.Success = true
						return true
					}
					return false
				}).Build()

				rf.mu.Lock()

				rf.syncEntriesToFollower(1, 3)
				So(actualArgs_1, ShouldResemble, expectArgs_1)
				So(actualArgs_2, ShouldResemble, expectArgs_2)

				So(rf.matchIdx, ShouldResemble, []int{6, 6, 6})
				So(rf.nextIdx, ShouldResemble, []int{7, 7, 7})

				rf.mu.Unlock()
			})

			PatchConvey("conflictIndex is less than snapshotIdx", func() {
				var actualArgs_1, actualArgs_2 *AppendEntriesArgs

				callCnt := 0
				Mock((*Raft).sendAppendEntries).To(func(rf *Raft, server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
					reply.Term = 3
					// 第一次调用 Success 失败, prevLogIndex=6, prevLogTerm=3 与 Follower 本地日志不匹配
					if callCnt == 0 {
						actualArgs_1 = args
						callCnt++
						reply.Success = false
						reply.ConflictIdx = 2
						reply.ConflictTerm = NewInt(1)
						return true
					}

					if callCnt == 1 {
						actualArgs_2 = args
						reply.Success = true
						return true
					}
					return false
				}).Build()

				rf.mu.Lock()

				rf.syncEntriesToFollower(1, 3)
				So(actualArgs_1, ShouldResemble, expectArgs_1)
				So(actualArgs_2, ShouldResemble, expectArgs_2)

				So(rf.matchIdx, ShouldResemble, []int{6, 6, 6})
				So(rf.nextIdx, ShouldResemble, []int{7, 7, 7})

				rf.mu.Unlock()
			})
		})
	})
}

func TestSyncBySnapshot(t *testing.T) {
	PatchConvey("Test SyncBySnapshot", t, func() {
		persister := MakePersister()

		rf := &Raft{
			me:          0,
			persister:   persister,
			peers:       []*labrpc.ClientEnd{{}, {}, {}},
			currentTerm: 4,
			log: []*LogEntry{
				{Term: 3},               // idx: 8
				{Term: 3, Command: "P"}, // idx: 9
				{Term: 4, Command: "Q"}, // idx: 10
				{Term: 4, Command: "R"}, // idx: 11
			},
			snapshotIdx: 8,
			nextIdx:     []int{12, 8, 8},
			matchIdx:    []int{11, 0, 0},
		}

		PatchConvey("sending InstallSnapshot RPC success", func() {

			Mock((*Raft).sendInstallSnapshot).To(func(rf *Raft, server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
				reply.Term = 3
				return true
			}).Build()

			rf.mu.Lock()
			rf.syncEntriesToFollower(1, 4)
			rf.mu.Unlock()

			So(rf.nextIdx[1], ShouldEqual, 9)
			So(rf.matchIdx[1], ShouldEqual, 8)
		})

		PatchConvey("sending InstallSnapshot RPC success, but the installation failed", func() {
			cnt := 0
			var arg1, arg2 *InstallSnapshotArgs
			var nextIdx_2 int
			Mock((*Raft).sendInstallSnapshot).To(func(rf *Raft, server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
				if cnt == 0 {
					arg1 = args
					reply.Term = 3
					cnt++
					return true
				} else if cnt == 1 {
					nextIdx_2 = rf.nextIdx[server]
					arg2 = args
					atomic.StoreInt32(&rf.dead, 1) // kill raft
					return false
				}
				return false
			}).Build()

			var arg3 *AppendEntriesArgs
			Mock((*Raft).sendAppendEntries).To(func(rf *Raft, server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
				arg3 = args
				reply.Success = false
				reply.ConflictIdx = 5 // the term of follower log index 5~8 is 2
				reply.ConflictTerm = NewInt(2)
				reply.Term = 2
				return true
			}).Build()

			// first of all, send InstallSnapshot RPC
			// and then, send AppendEntries RPC, return failed
			go rf.logSyncer(1, 4)

			time.Sleep(2 * time.Second)

			expectInstallArgs := &InstallSnapshotArgs{
				Term:      4,
				LeaderId:  0,
				LastIndex: 8,
				LastTerm:  3,
				Snapshot:  nil,
			}
			expectAppendArgs := &AppendEntriesArgs{
				Term:        4,
				LeaderId:    0,
				PrevLogIdx:  8,
				PrevLogTerm: 3,
				Entries: []*LogEntry{
					{Term: 3, Command: "P"}, // idx: 9
					{Term: 4, Command: "Q"}, // idx: 10
					{Term: 4, Command: "R"}, // idx: 11
				},
			}

			So(rf.killed(), ShouldEqual, true)
			So(arg1, ShouldResemble, expectInstallArgs)
			So(arg2, ShouldResemble, expectInstallArgs)
			So(arg3, ShouldResemble, expectAppendArgs)

			So(nextIdx_2, ShouldEqual, 8)
		})
	})
}
