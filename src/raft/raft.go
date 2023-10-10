package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"github.com/bytedance/sonic"
	"log"
	//	"bytes"
	"sync"
	"sync/atomic"
	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

// ApplyMsg 当新的日志条目被提交后, 通过Make()入参applyCh, 将该结构体传递给服务(或测试程序)
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int // 注意: CommandIndex 起始为 1

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int // 生成日志条目的任期
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	/* 持久化状态 */
	currentTerm  int  // 当前任期
	votedFor     *int // 给 peers[] 中的哪个成员投票(设置成唯一ID)
	log          []*LogEntry
	snapshotIdx  int // 快照中最后一个条目的索引
	snapshotTerm int // 快照中最后一个条目的任期

	/* 易失状态 */
	status      NodeStateEnum
	commitIdx   int // 本地已被提交的日志(初始值0)
	lastApplied int // 本地以及被应用到状态机的条目(初始值0)

	/* 仅Leader保存 */
	nextIdx  []int // 成员下一条需要接收的日志条目索引(初始值 日志最新条目的索引值+1)
	matchIdx []int // 成员与Leader同步的日志条目的最大索引

	/* Goroutine 通讯 */
	// 状态切换为 Follower, 通过该变量通知 ticker
	toFollower *sync.Cond

	// 新的日志被提交, 发送 ApplyMsg
	logCommit *sync.Cond

	// 用于重置 Follower 的选举超时计时器, 在 listenResetTimer 中被使用, channel 传输任期
	// eg: Follower currentTerm=2, 出现如下事件之一, 向 resetTimer 发送任期 2
	// 1. 收到当前或更高任期的 Leader 发送的 AppendEntries RPC
	// 2. RequestVote RPC 投赞成票
	resetSelectionTimer *BufferIntChan

	// Candidate 监听 resetCandidate, 重置 candidate 状态
	// 当收到新Leader的心跳 或 更高任期的RPC, 节点从 Candidate 切换为 Follower
	// 向 resetCandidate 传递任期, 告知节点结束选举, 退出 rf.candidate Goroutine
	// eg: Candidate currentTerm=2, 出现如下事件之一, 则传递任期2到channel
	// 1. 收到任期 >=2 的 AppendEntries RPC
	// 2. 收到任期 > 2 的 RequestVote RPC
	resetCandidate *BufferIntChan
}

// GetState return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.status == NodeStateEnum_Leader
	term = rf.currentTerm

	// Your code here (2A).
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// Note: call persist() while holding the mutex rf.mu
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)

	err := encoder.Encode(rf.currentTerm)
	if err != nil {
		DPrintf(dError, LogCommonFormat+", encoder.Encode(currentStatus) failed, err=%v",
			[]interface{}{rf.me, rf.status, rf.currentTerm, err})
		return
	}

	// 注: gob 不能序列化空指针
	err = encoder.Encode(IntPtrToVal(rf.votedFor, InitVotedFor))
	if err != nil {
		DPrintf(dError, LogCommonFormat+", encoder.Encode(votedFor) failed, err=%v",
			[]interface{}{rf.me, rf.status, rf.currentTerm, err})
		return
	}

	err = encoder.Encode(rf.log)
	if err != nil {
		DPrintf(dError, LogCommonFormat+", encoder.Encode(rf.log) failed, err=%v",
			[]interface{}{rf.me, rf.status, rf.currentTerm, err})
		return
	}

	// 序列化Raft的持久化状态
	data := buffer.Bytes()

	rf.persister.SaveRaftState(data)
	DPrintf(dPersist, LogCommonFormat+" rf.persist, votedFor S%v, LastLogEntry(Idx%v, T%v, Command %v)",
		[]interface{}{rf.me, rf.status, rf.currentTerm, IntPtrToVal(rf.votedFor, InitVotedFor),
			rf.getGlobalIndex(len(rf.log) - 1), rf.log[len(rf.log)-1].Term, rf.log[len(rf.log)-1].Command})
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)

	var (
		currentTerm int
		votedFor    int
		logEntries  []*LogEntry
	)

	// 入参一定要为指针类型
	err := decoder.Decode(&currentTerm)
	if err != nil {
		DPrintf(dError, "decoder.Decode(&currentTerm) failed, err=%v",
			[]interface{}{err})
		return
	}
	err = decoder.Decode(&votedFor)
	if err != nil {
		DPrintf(dError, "decoder.Decode(&votedFor) failed, err=%v",
			[]interface{}{err})
		return
	}
	err = decoder.Decode(&logEntries)
	if err != nil {
		DPrintf(dError, "decoder.Decode(&votedFor) failed, err=%v",
			[]interface{}{err})
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm = currentTerm
	if votedFor != InitVotedFor {
		rf.votedFor = NewInt(votedFor)
	}
	rf.log = logEntries

	// TODO 恢复快照的索引和任期

	DPrintf(dPersist, LogCommonFormat+" rf.readPersist, votedFor S%v, LastLogEntry(Idx%v, T%v, Command %v)",
		[]interface{}{rf.me, NodeStateEnum_Follower, currentTerm, votedFor,
			rf.getGlobalIndex(len(logEntries) - 1), rf.log[len(logEntries)-1].Term, rf.log[len(logEntries)-1].Command})
	return
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int // 若没有日志条目, 设置为 -1
	LastLogTerm  int // 若没有日志条目, 设置为 -1
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term  int  // 响应RPC的成员任期
	Voted bool // 是否投票赞成
}

// RequestVote RPC Handler
// 比较最新一条日志的任期, 若投票发起者任期更新, 投赞成票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args == nil || reply == nil {
		log.Panicf("RequestVote Handler, Invalid args or reply")
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	currentTerm := rf.currentTerm
	currentStatus := rf.status

	argStr, _ := sonic.MarshalString(args)
	logArgs := []interface{}{rf.me, currentStatus.String(), currentTerm, argStr}
	DPrintf(dVote, LogCommonFormat+", RequestVote Args=%v", logArgs)

	var voteFor *int
	reply.Term = currentTerm
	reply.Voted = false

	if args.Term < rf.currentTerm ||
		(args.Term == rf.currentTerm && rf.status != NodeStateEnum_Follower) {
		// args.Term 必须大于等于 currentTerm
		// 若等于, 当前节点必须为 Follower, 否则不可能投赞成票
		return
	}

	// 若当前任期 小于 请求发送者的任期, 执行如下操作：
	// 1. 任期更新为args.Term, 切换为 Follower
	// 2. 比较日志, 如果 RPC 发起者更新, 投下赞同票
	if args.Term > currentTerm {
		if rf.upToDate(args.LastLogIndex, args.LastLogTerm) {
			// 发起者的日志最新, 投赞成票
			voteFor = NewInt(args.CandidateId)
		}
		// 必须在锁保护的情况下, 调用 rf.follower
		rf.follower(args.Term, voteFor)
	}

	if args.Term == currentTerm {
		// rf.voteFor 为空或等于 candidateId, 且发起者的日志最新, 投赞成票
		if IntPtrToVal(rf.votedFor, args.CandidateId) == args.CandidateId &&
			rf.upToDate(args.LastLogIndex, args.LastLogTerm) {
			voteFor = NewInt(args.CandidateId)
			rf.votedFor = voteFor
			rf.persist() // persistence of votedFor
		}
	}

	if voteFor != nil {
		logArgs = append(logArgs[:3], *voteFor)
		DPrintf(dVote, LogCommonFormat+", Vote S%v!", logArgs)
		reply.Voted = true
		// 在本次 RequestVote 中投下赞成票, 重置 Follower 计时器, eventTerm 设置为 currentTerm
		if currentStatus == NodeStateEnum_Follower {
			go func() {
				ok := rf.resetSelectionTimer.Send(currentTerm)
				if !ok {
					DPrintf(dError, LogCommonFormat+", RequestVote Handler, rf.resetSelectionTimer.Send failed", logArgs[:3])
					return
				}
				DPrintf(dHeartbeat, LogCommonFormat+", RequestVote Handler, rf.resetSelectionTimer.Send success", logArgs[:3])
			}()
		}
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int // Leader 任期
	LeaderId     int
	PrevLogIdx   int         // 新日志前条目的索引
	PrevLogTerm  int         // 该条目的任期
	Entries      []*LogEntry // 新日志条目
	LeaderCommit int         // Leader 节点最后一个提交条目的索引
}

type AppendEntriesReply struct {
	Term         int  // 接收者任期
	Success      bool // 是否成功
	ConflictTerm *int
	ConflictIdx  int // 冲突日志的索引
}

// AppendEntries
//  1. 如果Term小于currentTerm, 直接返回 false;
//     1.1 更新当前任期和状态 , 执行一致性检查
//  2. 如果 prevLogIndex, prevLogTerm 与 当前节点的日志不匹配, 返回 false
//     2.1 如果不存在冲突的日志条目, conflictTerm 设置为 nil, conflictIdx 为最新日志条目的索引
//     2.2 如果存在冲突条目, conflictTerm 为冲突条目的任期, conflictIdx 为任期 conflictTerm 的第一个日志条目
//  3. 如果已经存在的日志条目 与 新的日志条目存在冲突, 删除本地日志中冲突索引及之后的条目
//  4. 复制当前日志中, 不存在的新日志条目
//  5. commitIndex 设置为 min(LeaderCommit, 最后一个新日志条目的索引)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args == nil || reply == nil {
		log.Panic("AppendEntries Handler, invalid args or reply")
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	currentTerm := rf.currentTerm
	currentStatus := rf.status

	argStr, _ := sonic.MarshalString(args)
	logArgs := []interface{}{rf.me, currentStatus.String(), currentTerm, argStr}

	DPrintf(dAppend, LogCommonFormat+", AppendEntries Args=%v", logArgs)
	reply.Term = currentTerm
	reply.Success = false

	// args.Term 小于 currentTerm, 直接返回 false
	if currentTerm > args.Term {
		return
	}

	if args.Term > currentTerm {
		rf.follower(args.Term, NewInt(args.LeaderId))
	}

	if args.Term == currentTerm {
		if currentStatus == NodeStateEnum_Candidate {
			rf.follower(args.Term, rf.votedFor)
		} else if currentStatus == NodeStateEnum_Leader {
			log.Panicf(LogCommonFormat+", AppendEntries Handler, Multiple Leader in just one term", logArgs[:3]...)
		}
	}

	// 心跳 RPC, 重置 Follower 计时器
	if currentStatus == NodeStateEnum_Follower {
		go func() {
			ok := rf.resetSelectionTimer.Send(currentTerm)
			if !ok {
				DPrintf(dError, LogCommonFormat+", AppendEntries Handler, rf.resetSelectionTimer.Send failed", logArgs[:3])
				return
			}
			DPrintf(dHeartbeat, LogCommonFormat+", AppendEntries Handler, rf.resetSelectionTimer.Send success", logArgs[:3])
		}()
	}

	curLastLogIndex := rf.getGlobalIndex(len(rf.log) - 1)
	if curLastLogIndex < args.PrevLogIdx {
		reply.ConflictIdx = curLastLogIndex
		replyStr, _ := sonic.MarshalString(reply)
		DPrintf(dAppend, LogCommonFormat+", AppendEntries Handler, reply=%v",
			[]interface{}{rf.me, currentStatus.String(), rf.currentTerm, replyStr})
		return
	}

	localPrevLogIdx := rf.getLocalIndex(args.PrevLogIdx)
	localPrevLogTerm := rf.log[localPrevLogIdx].Term
	if localPrevLogTerm != args.PrevLogTerm {
		conflictIdx := localPrevLogIdx
		for idx := localPrevLogIdx; idx > 0; idx-- {
			if rf.log[idx-1].Term == localPrevLogTerm {
				conflictIdx = idx - 1
			} else {
				break
			}
		}
		reply.ConflictIdx = rf.getGlobalIndex(conflictIdx)
		reply.ConflictTerm = NewInt(localPrevLogTerm)

		replyStr, _ := sonic.MarshalString(reply)
		DPrintf(dAppend, LogCommonFormat+", AppendEntries Handler, reply=%v",
			[]interface{}{rf.me, currentStatus.String(), rf.currentTerm, replyStr})
		return
	}

	// 通过一致性检查, 更新本地日志
	reply.Success = true
	conflictIdx := (*int)(nil)       // 第一个冲突条目的索引
	appendBegin := len(args.Entries) // args.Entries 中索引最小的新条目
	for i, entry := range args.Entries {
		logIdx := localPrevLogIdx + 1 + i
		if logIdx < len(rf.log) && rf.log[logIdx].Term != entry.Term {
			conflictIdx = NewInt(logIdx)
			appendBegin = i
			break
		}
	}

	suffixLength := len(rf.log) - 1 - localPrevLogIdx
	if conflictIdx != nil {
		// 从 conflictIdx 开始, 删除本地日志条目
		rf.deleteEntryFrom(*conflictIdx)
	} else if suffixLength < len(args.Entries) {
		appendBegin = suffixLength
	}

	for i := appendBegin; i < len(args.Entries); i++ {
		rf.log = append(rf.log, args.Entries[i])
	}
	rf.persist() // persistence of log

	// 更新 commitIdx, 通知 applier 应用新日志到状态机
	commitIdx := MinInt(args.LeaderCommit, args.PrevLogIdx+len(args.Entries))
	if commitIdx > rf.commitIdx {
		DPrintf(dCommit, LogCommonFormat+", commitIdx %v->%v",
			[]interface{}{rf.me, currentStatus.String(), rf.currentTerm, rf.commitIdx, commitIdx})
		rf.commitIdx = commitIdx
		rf.logCommit.Broadcast()
	}

	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Start the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	index = -1
	term = -1
	isLeader = true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != NodeStateEnum_Leader {
		isLeader = false
		return
	}

	// 新增日志条目, logSyncer 每隔 15ms 检查是否有新增条目, 若有则发起 AppendEntries RPC
	term = rf.currentTerm
	index = rf.getGlobalIndex(len(rf.log))
	entry := &LogEntry{
		Term:    term,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	rf.matchIdx[rf.me] = index
	rf.nextIdx[rf.me] = index + 1
	rf.persist() // persistence of log

	DPrintf(dLog, LogCommonFormat+", submit a new entry(index %v), command=%v",
		[]interface{}{rf.me, rf.status.String(), term, index, command})

	return
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.

	// 退出ticker, applier Goroutine
	rf.toFollower.Broadcast()
	rf.logCommit.Broadcast()

	// 释放BufferChan
	rf.resetSelectionTimer.Kill()
	DPrintf(dKill, "S%v rf.resetSelectionTimer killed", []interface{}{rf.me})
	rf.resetCandidate.Kill()
	DPrintf(dKill, "S%v rf.resetCandidate killed", []interface{}{rf.me})
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election
// if this peer hasn't received heartbeats recently.
// 1. 若不为 Follower, 等待节点成为Follower
// 2. 若为 Follower, 启动定时器, 超时则开启选举
// 3. 若收到resetTimer的信号, 重置定时器
func (rf *Raft) ticker() {

	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// 获取任期和状态
		rf.mu.Lock()
		currentTerm := rf.currentTerm
		status := rf.status

		logArgs := []interface{}{rf.me, status.String(), currentTerm}

		switch status {
		case NodeStateEnum_Follower:
			// 在等待定时器超时前, 释放锁
			rf.mu.Unlock()

			timeout := make(chan struct{}, 1) // buffered channel, 防止goroutine泄漏

			resetTimer := make(chan int)
			// 设置为unbuffered, 在send exit返回后本轮loop listenResetSelectionTimer已经退出, 防止多协程消费rf.resetTimer
			exit := make(chan struct{})

			// 启动定时器 Goroutine
			go startTimer(timeout, TimerScene_Selection)

			// 启动 定时器重置事件 监听器
			go rf.listenResetTimer(ResetTimerScene_SelectionTimer, resetTimer, exit, currentTerm)

			select {
			case <-timeout:
				exit <- struct{}{}
				// 采用同步的方式, 确保成员状态和任期的更新
				// 若异步进行存在问题:
				// 1. ticker 可能在 rf.candidate 前再次获取锁, 导致 ticker 计时器和 candidate 计时器同时启动
				//   ticker 和 candidate 计时器同一时间只启动一个
				DPrintf(dHeartbeat, LogCommonFormat+", Heartbeat timeout", logArgs)
				rf.candidate(currentTerm)
			case eventTerm, _ := <-resetTimer:
				logArgs = append(logArgs, eventTerm)
				DPrintf(dHeartbeat, LogCommonFormat+", Reset heartbeat timer, eventT:%v", logArgs)
				// 1. 响应RequestVote RPC, 并投赞成票
				// 2. 接收到当前任期Leader的AppendEntries RPC
			}
		case NodeStateEnum_Leader, NodeStateEnum_Candidate:
			// 等待切换为Follower
			rf.toFollower.Wait()
			DPrintf(dInfo, LogCommonFormat+", ticker ToFollower Signal Received", logArgs)
			rf.mu.Unlock()
		default:
			rf.mu.Unlock()
			log.Panicf(LogCommonFormat+", ticker unkonwn rf.status", logArgs...)
		}
	}
}

// Make the service or tester wants to create a Raft server.
// 1. the ports of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order.
// 2. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any.
// 3. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.log = []*LogEntry{{}} // 日志的索引 0 处的日志条目为 dummyEntry (见 config.go applier 和 applierSnap)
	rf.status = NodeStateEnum_Follower
	rf.commitIdx = InitLogIndex
	rf.lastApplied = InitLogIndex

	rf.resetSelectionTimer = NewBufferChan(make(chan int), make(chan int))
	go rf.resetSelectionTimer.Run()
	rf.resetCandidate = NewBufferChan(make(chan int), make(chan int))
	go rf.resetCandidate.Run()

	rf.toFollower = sync.NewCond(&rf.mu)
	rf.logCommit = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 启动 applier Goroutine, 将提交的日志条目应用到状态机
	go rf.applier()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
