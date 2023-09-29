package raft

import (
	"github.com/bytedance/sonic"
	"log"
)

// candidate 开启一个选举
// 1. 自增currentTerm
// 2. 投票给自己
// 3. 向集群中其他成员发送 RequestVote RPC
// 4. 可能的结果:
//	4.1 若选举超时, 开启新的选举;
//  4.2 若收到超过半数的赞成票, 当选Leader;
//  4.3 若收到新Leader的AppendEntries RPC,切换为Follower

// candidate 是否快照任期作为入参？(应该不需要)
// 存在任期改变, 但不需要重置定时器的情况
// RequestVote 投赞成票, 重置Timer 和 切换Follower并发, 可能导致expectTerm小于当前任期
func (rf *Raft) candidate() {
	rf.mu.Lock()

	logArgs := []interface{}{rf.me, rf.status.String(), rf.currentTerm}
	DPrintf(dStatusSwitch, LogCommonFormat+", Switch To Candidate", logArgs)

	var (
		lastLogIdx  = len(rf.log) - 1
		lastLogTerm = -1                            // 若没有日志条目, 设置为-1
		voteCh      = make(chan int, len(rf.peers)) // RequestVote Handler协程传递收到的赞成票
		voterSet    = make(map[int]struct{})        // 用于统计投票数, 具有去重的作用
	)

	rf.status = NodeStateEnum_Candidate
	rf.currentTerm++
	rf.votedFor = NewInt(rf.me)
	voterSet[rf.me] = struct{}{}
	logArgs[1] = NodeStateEnum_Candidate.String()

	if lastLogIdx >= 0 {
		lastLogTerm = rf.log[lastLogIdx].Term
	}

	voteReq := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIdx,
		LastLogTerm:  lastLogTerm,
	} // req结构体: 只读

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.requestVoteTask(i, voteReq, voteCh)
	}
	rf.mu.Unlock()

	timeout := make(chan struct{}, 1) // timer协程传递超时信号
	// 定时器 Goroutine
	go startTimer(timeout, TimerScene_Selection)

	resetCandidate := make(chan int)
	exit := make(chan struct{}) // listenResetCandidate 退出
	// 启动监听rf.resetCandidate Goroutine
	go rf.listenResetTimer(ResetTimerScene_Candidate, resetCandidate, exit, voteReq.Term)

	for len(voterSet) <= len(rf.peers)/2 {
		if rf.killed() {
			exit <- struct{}{}
			return
		}

		select {
		case voter := <-voteCh:
			// 收到赞成票
			logArgs = append(logArgs[:3], voter)
			DPrintf(dVote, LogCommonFormat+", Receive S%v vote", logArgs)
			voterSet[voter] = struct{}{}
		case <-timeout:
			// 选举超时, 重新开启选举
			DPrintf(dSelection, LogCommonFormat+", Selection Timeout!", logArgs[:3])
			exit <- struct{}{}
			rf.candidate()
			return
		case eventTerm := <-resetCandidate:
			logArgs = append(logArgs[:3], eventTerm)
			DPrintf(dSelection, LogCommonFormat+", receive ResetCandidate, cancel Selection, event T%v", logArgs)
			return
		}
	}

	exit <- struct{}{}
	DPrintf(dSelection, LogCommonFormat+", win Selection!", logArgs[:3])
	// 获取了集群中大多数节点的投票, 当选Leader
	go rf.leader(voteReq.Term)
}

func (rf *Raft) requestVoteTask(server int, voteReq *RequestVoteArgs, voteCh chan<- int) {
	reqStr, _ := sonic.MarshalString(voteReq)
	DPrintf(dVote, "S%v send RequestVote to S%v, voteReq=%v", []interface{}{rf.me, server, reqStr})

	voteResp := &RequestVoteReply{}
	ok := rf.sendRequestVote(server, voteReq, voteResp)
	if !ok {
		DPrintf(dError, "S%v T%v, send RequestVote to S%v failed", []interface{}{rf.me, voteReq.Term, server})
		return
	}

	respStr, _ := sonic.MarshalString(voteResp)
	DPrintf(dVote, "S%v T%v, receive RequestVote reply from S%v, reply=%v",
		[]interface{}{rf.me, voteReq.Term, server, respStr})

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm < voteResp.Term {
		// 响应数据中的任期大于当前任期, 切换为Follower
		rf.follower(voteResp.Term, nil)
		return
	}

	// 请求时的任期 和 节点当前任期不同, 直接返回
	if rf.currentTerm != voteReq.Term {
		return
	}

	// 被投赞成票, 累加计数器
	if voteResp.Voted {
		voteCh <- server
	}
}

// follower 切换为 Follower 节点
// 入参: targetTerm 状态切换的目标任期; voteFor 任期 targetTerm 内, 给哪位成员赞成票
// 状态切换为 Follower 后, 唤醒阻塞在条件变量 rf.toFollower 的 Goroutine, 重启定时器
// 注: 在持有 rf.mu 时才调用该方法, follower 不在方法中加锁的原因是 RPC Handler 中进行状态切换时已经持有了锁。
func (rf *Raft) follower(targetTerm int, voteFor *int) {
	currentTerm := rf.currentTerm
	currentStatus := rf.status

	logArgs := []interface{}{rf.me, currentStatus.String(), currentTerm, targetTerm, IntPtrToVal(voteFor, -1)}
	DPrintf(dStatusSwitch, LogCommonFormat+", Switch To Follower, target T%v, voteFor S%v", logArgs)

	if currentTerm > targetTerm {
		// 忽略目标任期小于当前任期的状态切换
		return
	}

	if currentTerm == targetTerm && currentStatus != NodeStateEnum_Candidate {
		// 任期不变, 切换前状态必须为Candidate
		return
	}

	rf.currentTerm = targetTerm
	rf.status = NodeStateEnum_Follower
	rf.votedFor = voteFor
	logArgs[1] = NodeStateEnum_Follower.String()

	switch currentStatus {
	case NodeStateEnum_Leader:
		// 结束rf.leader Goroutine
		go func() {
			ok := rf.resetLeader.Send(currentTerm)
			if !ok {
				DPrintf(dError, LogCommonFormat+", (*Raft).follower send ResetLeader failed", logArgs[:3])
				return
			}
			DPrintf(dStatusSwitch, LogCommonFormat+", (*Raft).follower send ResetLeader success", logArgs[:3])
		}()
	case NodeStateEnum_Candidate:
		// 正在执行选举, 通知 rf.listenResetCandidate, 终止选举过程
		go func() {
			ok := rf.resetCandidate.Send(currentTerm)
			if !ok {
				DPrintf(dError, LogCommonFormat+", (*Raft).follower send ResetCandidate failed", logArgs[:3])
				return
			}
			DPrintf(dStatusSwitch, LogCommonFormat+", (*Raft).follower send ResetCandidate success", logArgs[:3])
		}()
	case NodeStateEnum_Follower:
		return
	default:
		log.Panicf(LogCommonFormat+", (*Raft).follower Invalid status", logArgs[:3]...)
	}

	// 若节点由 Leader 或 Candidate 切换而来, 唤醒阻塞在 toFollower 上的 ticker Goroutine
	rf.toFollower.Broadcast()
}

// leader candidate()赢得选举后, 切换为Leader状态
// expectTerm 为 Leader 的任期
func (rf *Raft) leader(expectTerm int) {
	rf.mu.Lock()

	currentTerm := rf.currentTerm
	logArgs := []interface{}{rf.me, rf.status.String(), currentTerm, expectTerm}
	DPrintf(dStatusSwitch, LogCommonFormat+", Switch To Leader, expect T%v", logArgs)

	if currentTerm != expectTerm {
		rf.mu.Unlock()
		return
	}

	// Leader 节点状态初始化
	rf.status = NodeStateEnum_Leader
	rf.nextIdx = make([]int, len(rf.peers))
	rf.matchIdx = make([]int, len(rf.peers))
	for i, _ := range rf.matchIdx {
		rf.matchIdx[i] = -1
	}

	rf.mu.Unlock()
	logArgs[1] = NodeStateEnum_Leader.String()
	// 每个 for-loop 依次进行如下工作:
	// 1. 启动多个 Goroutine, 向所有成员发送心跳 RPC
	// 2. 启动心跳超时计时器, 当计时器超时后, 结束当前 loop
	// 3. 在监听定时器超时的同时, 监听 resetLeader channel
	// 	resetLeader 事件由 rf.follower发出, 用于结束 rf.leader Goroutine
	for !rf.killed() {
		for server, _ := range rf.peers {
			if server == rf.me {
				continue
			}
			go rf.heartbeat(server, currentTerm)
		}

		// 心跳周期计时器
		timeout := make(chan struct{}, 1)
		go startTimer(timeout, TImerScene_Heartbeat)

		resetLeader := make(chan int)
		exit := make(chan struct{})
		go rf.listenResetTimer(ResetTimerScene_Leader, resetLeader, exit, currentTerm)

		select {
		case <-timeout:
			// 开始新的心跳周期
			exit <- struct{}{}
			DPrintf(dHeartbeat, LogCommonFormat+", Begin a new round of heartbeat rpc", logArgs[:3])
		case eventTerm := <-resetLeader:
			logArgs = append(logArgs[:3], eventTerm)
			DPrintf(dStatusSwitch, LogCommonFormat+", (*Raft).leader receive ResetLeader, event T%v", logArgs)
			return
		}
	}
}

// heartbeat 向成员 server 发送心跳
// TODO 2B 实验补充日志相关内容
func (rf *Raft) heartbeat(server int, argTerm int) {
	args := &AppendEntriesArgs{
		Term:         argTerm,
		LeaderId:     rf.me,
		PrevLogIdx:   InitLogIndex,
		PrevLogTerm:  InitLogTerm,
		Entries:      make([]*LogEntry, 0),
		LeaderCommit: InitLogIndex,
	}
	argStr, _ := sonic.MarshalString(args)
	DPrintf(dHeartbeat, "S%v send Heartbeat rpc to S%v, args=%v", []interface{}{rf.me, server, argStr})

	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	if !ok {
		DPrintf(dError, "S%v T%v, send Heartbeat to S%v failed", []interface{}{rf.me, argTerm, server})
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	replyStr, _ := sonic.MarshalString(reply)
	currentTerm := rf.currentTerm
	DPrintf(dHeartbeat, "S%v T%v, receive Heartbeat reply from S%v, replyStr=%v",
		[]interface{}{rf.me, currentTerm, server, replyStr})

	if reply.Term > currentTerm {
		// 当前节点为过去任期, 切换为 Follower, 更新任期
		rf.follower(reply.Term, nil)
		return
	}

	if currentTerm != argTerm {
		// 当前任期不等于发出请求时的任期, 不请求响应结果
		return
	}
	// TODO
}
