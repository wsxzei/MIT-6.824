package raft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	. "github.com/bytedance/mockey"
	. "github.com/smartystreets/goconvey/convey"
	"sync"
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
			resetCandidate:      NewBufferChan(make(chan int), make(chan int)),
		}
		go rf.resetSelectionTimer.Run()
		go rf.resetCandidate.Run()

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
	PatchConvey("Test AppendEntries", t, func() {
		rf := &Raft{
			me:                  0,
			peers:               []*labrpc.ClientEnd{},
			currentTerm:         2,
			status:              NodeStateEnum_Follower,
			resetSelectionTimer: NewBufferChan(make(chan int), make(chan int)),
			resetCandidate:      NewBufferChan(make(chan int), make(chan int)),
			snapshotIdx:         2,
		}
		go rf.resetSelectionTimer.Run()
		go rf.resetCandidate.Run()

		PatchConvey("prevLogIdx 在本地日志中不存在", func() {
			// Index 	3		4		5
			// Leader	1		2		2
			// Follower 1		1
			rf.log = []*LogEntry{
				{Term: 0}, {Term: 1}, {Term: 1},
			}
			args := &AppendEntriesArgs{
				Term:        3,
				LeaderId:    1,
				PrevLogIdx:  5,
				PrevLogTerm: 2,
				Entries:     []*LogEntry{},
			}
			reply := &AppendEntriesReply{}

			rf.AppendEntries(args, reply)

			So(reply.Term, ShouldEqual, 2)
			So(reply.Success, ShouldEqual, false)
			So(reply.ConflictTerm, ShouldBeNil)
			So(reply.ConflictIdx, ShouldEqual, 4)

			// 检查状态切换
			rf.mu.Lock()
			So(rf.currentTerm, ShouldEqual, 3)
			So(rf.status, ShouldEqual, NodeStateEnum_Follower)
			rf.mu.Unlock()

			// 检查 重置心跳超时Timer 的消息
			eventTerm, _ := rf.resetSelectionTimer.Receive()
			So(eventTerm, ShouldEqual, 2)
		})

		PatchConvey("prevLogIndex 存在, 但 Term 不匹配", func() {
			// Index 	3		4		5
			// Leader	1		2		3
			// Follower 1		2       2
			rf.log = []*LogEntry{
				{Term: 0}, {Term: 1}, {Term: 2}, {Term: 2},
			}
			args := &AppendEntriesArgs{
				Term:        3,
				LeaderId:    1,
				PrevLogIdx:  5,
				PrevLogTerm: 3,
				Entries:     []*LogEntry{},
			}
			reply := &AppendEntriesReply{}

			rf.AppendEntries(args, reply)
			So(reply.Term, ShouldEqual, 2)
			So(reply.Success, ShouldEqual, false)
			So(reply.ConflictTerm, ShouldEqual, NewInt(2))
			So(reply.ConflictIdx, ShouldEqual, 4)

			// 检查状态切换
			rf.mu.Lock()
			So(rf.currentTerm, ShouldEqual, 3)
			So(rf.status, ShouldEqual, NodeStateEnum_Follower)
			rf.mu.Unlock()

			// 检查 重置心跳超时Timer 的消息
			eventTerm, _ := rf.resetSelectionTimer.Receive()
			So(eventTerm, ShouldEqual, 2)
		})

		PatchConvey("prevLogIndex 和 prevLogTerm 匹配", func() {
			rf.logCommit = sync.NewCond(&rf.mu)

			type testCase struct {
				args            *AppendEntriesArgs
				expectReply     *AppendEntriesReply
				expectLog       []*LogEntry
				expectCommitIdx int
				mock            func()
				testCond        bool
			}

			cases := []testCase{
				// 存在冲突日志, 删除原日志后 Append
				{
					// Index 	3		4		5
					// Leader	1		3		3
					// Follower 1		2       2
					args: &AppendEntriesArgs{
						Term:         3,
						LeaderId:     1,
						PrevLogIdx:   3,
						PrevLogTerm:  1,
						Entries:      []*LogEntry{{Term: 3, Command: 3}, {Term: 3, Command: 5}},
						LeaderCommit: 5,
					},
					expectReply: &AppendEntriesReply{
						Term:    2,
						Success: true,
					},
					expectLog:       []*LogEntry{{Term: 0}, {Term: 1, Command: 2}, {Term: 3, Command: 3}, {Term: 3, Command: 5}},
					expectCommitIdx: 5,
					mock: func() {
						rf.log = []*LogEntry{
							{Term: 0}, {Term: 1, Command: 2}, {Term: 2, Command: 4}, {Term: 2, Command: 6},
						}
						rf.commitIdx = 3
					},
					testCond: true,
				},
				// 无冲突日志, 仅 Append
				{
					// Index 	3		4		5		6
					// Leader	1		2		2		3
					// Follower 1		2       2
					args: &AppendEntriesArgs{
						Term:         3,
						LeaderId:     1,
						PrevLogIdx:   4,
						PrevLogTerm:  2,
						Entries:      []*LogEntry{{Term: 2, Command: 3}, {Term: 3, Command: 5}},
						LeaderCommit: 6,
					},
					expectReply: &AppendEntriesReply{
						Term:    2,
						Success: true,
					},
					expectLog:       []*LogEntry{{Term: 0}, {Term: 1, Command: 2}, {Term: 2, Command: 4}, {Term: 2, Command: 3}, {Term: 3, Command: 5}},
					expectCommitIdx: 6,
					mock: func() {
						rf.currentTerm = 2
						rf.status = NodeStateEnum_Follower
						rf.log = []*LogEntry{
							{Term: 0}, {Term: 1, Command: 2}, {Term: 2, Command: 4}, {Term: 2, Command: 3},
						}
						rf.commitIdx = 5
					},
					testCond: true,
				},
				// 无冲突日志, 无新日志, 本地日志不应该改变
				// Index 	3		4		5		6
				// Follower	1		2		2		3
				{
					args: &AppendEntriesArgs{
						Term:         3,
						LeaderId:     1,
						PrevLogIdx:   3,
						PrevLogTerm:  1,
						Entries:      []*LogEntry{{Term: 2, Command: 4}, {Term: 2, Command: 3}},
						LeaderCommit: 6,
					},
					expectReply: &AppendEntriesReply{
						Term:    3,
						Success: true,
					},
					expectLog: []*LogEntry{
						{Term: 0}, {Term: 1, Command: 2}, {Term: 2, Command: 4}, {Term: 2, Command: 3}, {Term: 3, Command: 4},
					},
					expectCommitIdx: 5, // commitIndex 不应该更新为 6, 因为 args.Entries 中不包含索引 6 的条目
					mock: func() {
						rf.currentTerm = 3
						rf.status = NodeStateEnum_Follower
						rf.log = []*LogEntry{
							{Term: 0}, {Term: 1, Command: 2}, {Term: 2, Command: 4}, {Term: 2, Command: 3}, {Term: 3, Command: 4},
						}
						rf.commitIdx = 4
					},
				},
			}

			for _, test := range cases {
				test.mock()
				reply := &AppendEntriesReply{}

				if test.testCond {
					// 测试 logCommit 通知
					signal := make(chan struct{}, 1)
					go func() {
						rf.mu.Lock()
						rf.logCommit.Wait()
						rf.mu.Unlock()
						signal <- struct{}{}
					}()

					time.Sleep(10 * time.Millisecond)
					rf.AppendEntries(test.args, reply)
					<-signal
				} else {
					rf.AppendEntries(test.args, reply)
				}

				So(reply, ShouldResemble, test.expectReply)

				rf.mu.Lock()
				So(rf.log, ShouldResemble, test.expectLog)
				So(rf.commitIdx, ShouldEqual, test.expectCommitIdx)
				rf.mu.Unlock()
			}
		})
	})
}

func TestTicker(t *testing.T) {

}

func TestPersistAndReadPersist(t *testing.T) {
	PatchConvey("Test Persister", t, func() {
		persister := MakePersister()

		rf := &Raft{
			me:        0,
			peers:     []*labrpc.ClientEnd{{}, {}, {}},
			persister: persister,

			currentTerm: 2,
		}

		rf2 := &Raft{
			me:        0,
			peers:     []*labrpc.ClientEnd{{}, {}, {}},
			persister: persister,
		}

		PatchConvey("rf.votedFor isn't nil", func() {
			rf.votedFor = NewInt(1)
			rf.log = []*LogEntry{{}, {Term: 1, Command: "Hello World"}, {Term: 1, Command: 3}}

			rf.persist()
			rf2.readPersist(persister.ReadRaftState())

			So(rf2.currentTerm, ShouldEqual, 2)
			So(*rf2.votedFor, ShouldEqual, 1)
			So(rf2.log, ShouldResemble, []*LogEntry{{}, {Term: 1, Command: "Hello World"}, {Term: 1, Command: 3}})
		})

		PatchConvey("rf.votedFor is nil", func() {
			rf.log = []*LogEntry{{}, {Term: 1, Command: "Hello World"}, {Term: 1, Command: 3}}

			rf.persist()
			rf2.readPersist(persister.ReadRaftState())

			So(rf2.currentTerm, ShouldEqual, 2)
			So(rf2.votedFor, ShouldBeNil)
			So(rf2.log, ShouldResemble, []*LogEntry{{}, {Term: 1, Command: "Hello World"}, {Term: 1, Command: 3}})
		})

		PatchConvey("rf.log includes only dummy entry", func() {
			rf.log = []*LogEntry{{}}

			rf.persist()
			rf2.readPersist(persister.ReadRaftState())

			So(rf2.currentTerm, ShouldEqual, 2)
			So(rf2.votedFor, ShouldBeNil)
			So(rf2.log, ShouldResemble, []*LogEntry{{}})
		})
	})
}

func TestCondInstallSnapshot(t *testing.T) {
	PatchConvey("CondInstallSnapshot Test", t, func() {
		persister := MakePersister()
		rf := &Raft{
			me:          0,
			votedFor:    NewInt(0),
			peers:       []*labrpc.ClientEnd{{}, {}, {}},
			persister:   persister,
			snapshotIdx: 6,
			log: []*LogEntry{
				{Term: 2},
				{3, "A"}, // idx: 7
				{3, "B"}, // idx: 8
				{3, "C"}, // idx: 9
				{4, "D"}, // idx: 10
			},
			currentTerm: 4,
			lastApplied: 6,
			commitIdx:   7,
		}

		PatchConvey("local log matches lastIncluded Term and Index", func() {
			buffer := new(bytes.Buffer)
			encoder := labgob.NewEncoder(buffer)
			_ = encoder.Encode("B")
			expectSnapshot := buffer.Bytes()
			shouldApply := rf.CondInstallSnapshot(3, 8, expectSnapshot)

			// check log entries and snapshotIdx
			expectRaft := &Raft{
				me:          0,
				votedFor:    NewInt(0),
				peers:       []*labrpc.ClientEnd{{}, {}, {}},
				persister:   persister,
				snapshotIdx: 8,
				log: []*LogEntry{
					{Term: 3},
					{3, "C"}, // idx: 9
					{4, "D"}, // idx: 10
				},
				currentTerm: 4,
				lastApplied: 8,
				commitIdx:   8,
			}

			So(rf.snapshotIdx, ShouldEqual, expectRaft.snapshotIdx)
			So(rf.log, ShouldResemble, expectRaft.log)
			So(rf.commitIdx, ShouldEqual, expectRaft.commitIdx)
			So(rf.lastApplied, ShouldEqual, expectRaft.lastApplied)
			So(shouldApply, ShouldEqual, true)

			// decode byte array into structured object, check the persistence of raftState and snapshot
			actualRaftState, actualSnapshot := persister.raftState, persister.snapshot
			buffer2 := bytes.NewBuffer(actualRaftState)
			decoder := labgob.NewDecoder(buffer2)

			var (
				currentTerm, votedFor, snapshotIdx int
				logEntries                         []*LogEntry
				actualCommand                      string
			)
			decoder.Decode(&currentTerm)
			decoder.Decode(&votedFor)
			decoder.Decode(&snapshotIdx)
			decoder.Decode(&logEntries)
			So(currentTerm, ShouldEqual, expectRaft.currentTerm)
			So(votedFor, ShouldEqual, *expectRaft.votedFor)
			So(snapshotIdx, ShouldEqual, expectRaft.snapshotIdx)
			So(logEntries, ShouldResemble, expectRaft.log)

			buffer2 = bytes.NewBuffer(actualSnapshot)
			decoder = labgob.NewDecoder(buffer2)
			decoder.Decode(&actualCommand)
			So(actualCommand, ShouldEqual, "B")
		})

		PatchConvey("local log doesn't match lastIncluded Term and Index (1)", func() {
			// local log doesn't match lastIncluded Term and Index because of the conflict of Term at the lastIncludedIndex
			rf.currentTerm = 5
			buffer := new(bytes.Buffer)
			encoder := labgob.NewEncoder(buffer)
			_ = encoder.Encode("F")
			expectSnapshot := buffer.Bytes()
			shouldApply := rf.CondInstallSnapshot(5, 8, expectSnapshot)

			expectRaft := &Raft{
				votedFor:    NewInt(0),
				snapshotIdx: 8,
				log: []*LogEntry{
					{Term: 5},
				},
				currentTerm: 5,
				lastApplied: 8,
				commitIdx:   8,
			}

			So(rf.snapshotIdx, ShouldEqual, expectRaft.snapshotIdx)
			So(rf.log, ShouldResemble, expectRaft.log)
			So(rf.commitIdx, ShouldEqual, expectRaft.commitIdx)
			So(rf.lastApplied, ShouldEqual, expectRaft.lastApplied)
			So(shouldApply, ShouldEqual, true)

			actualRaftState, actualSnapshot := persister.raftState, persister.snapshot
			buffer2 := bytes.NewBuffer(actualRaftState)
			decoder := labgob.NewDecoder(buffer2)

			var (
				currentTerm, votedFor, snapshotIdx int
				logEntries                         []*LogEntry
				actualCommand                      string
			)
			decoder.Decode(&currentTerm)
			decoder.Decode(&votedFor)
			decoder.Decode(&snapshotIdx)
			decoder.Decode(&logEntries)
			So(currentTerm, ShouldEqual, expectRaft.currentTerm)
			So(votedFor, ShouldEqual, *expectRaft.votedFor)
			So(snapshotIdx, ShouldEqual, expectRaft.snapshotIdx)
			So(logEntries, ShouldResemble, expectRaft.log)

			buffer2 = bytes.NewBuffer(actualSnapshot)
			decoder = labgob.NewDecoder(buffer2)
			decoder.Decode(&actualCommand)
			So(actualCommand, ShouldEqual, "F")
		})

		PatchConvey("local log doesn't match lastIncluded Term and Index (2)", func() {
			// local log doesn't match lastIncluded Term and Index because of len(rf.log) <= lastIncludedIndex
			rf.currentTerm = 5
			buffer := new(bytes.Buffer)
			encoder := labgob.NewEncoder(buffer)
			_ = encoder.Encode("G")
			expectSnapshot := buffer.Bytes()
			shouldApply := rf.CondInstallSnapshot(5, 12, expectSnapshot)

			expectRaft := &Raft{
				votedFor:    NewInt(0),
				snapshotIdx: 12,
				log: []*LogEntry{
					{Term: 5},
				},
				currentTerm: 5,
				lastApplied: 12,
				commitIdx:   12,
			}

			So(rf.snapshotIdx, ShouldEqual, expectRaft.snapshotIdx)
			So(rf.log, ShouldResemble, expectRaft.log)
			So(rf.commitIdx, ShouldEqual, expectRaft.commitIdx)
			So(rf.lastApplied, ShouldEqual, expectRaft.lastApplied)
			So(shouldApply, ShouldEqual, true)
		})
	})
}

func TestInstallSnapshot(t *testing.T) {
	PatchConvey("Test InstallSnapshot", t, func() {
		persister := MakePersister()
		rf := &Raft{
			me:                  1,
			persister:           persister,
			currentTerm:         4,
			log:                 []*LogEntry{{}},
			status:              NodeStateEnum_Follower,
			applyCh:             make(chan ApplyMsg),
			resetSelectionTimer: NewBufferChan(make(chan int), make(chan int)),
		}
		go rf.resetSelectionTimer.Run()

		PatchConvey("send snapshot", func() {
			buff := new(bytes.Buffer)
			encoder := labgob.NewEncoder(buff)
			_ = encoder.Encode("E")
			snapshot := buff.Bytes()

			args := &InstallSnapshotArgs{
				Term:      5,
				LeaderId:  0,
				LastIndex: 10,
				LastTerm:  5,
				Snapshot:  snapshot,
			}
			reply := &InstallSnapshotReply{}

			rf.InstallSnapshot(args, reply)

			So(rf.currentTerm, ShouldEqual, 5)
			So(*rf.votedFor, ShouldEqual, 0)

			time.Sleep(10 * time.Millisecond)

			applyMsg := <-rf.applyCh
			So(applyMsg, ShouldResemble, ApplyMsg{
				SnapshotValid: true,
				Snapshot:      snapshot,
				SnapshotTerm:  5,
				SnapshotIndex: 10,
			})

			eventTerm, ok := rf.resetSelectionTimer.Receive()
			So(ok, ShouldEqual, true)
			So(eventTerm, ShouldEqual, 4)

			var (
				currentTerm, votedFor, snapshotIdx int
				logEntries                         []*LogEntry
			)
			actualRaftState := persister.raftState
			buffer := bytes.NewBuffer(actualRaftState)
			decoder := labgob.NewDecoder(buffer)
			decoder.Decode(&currentTerm)
			decoder.Decode(&votedFor)
			decoder.Decode(&snapshotIdx)
			decoder.Decode(&logEntries)
			So(currentTerm, ShouldEqual, 5)
			So(votedFor, ShouldEqual, 0)
			So(snapshotIdx, ShouldEqual, 0)
			So(logEntries, ShouldResemble, rf.log)
		})

	})
}
