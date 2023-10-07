package raft

import (
	"github.com/bytedance/sonic"
	"time"
)

// upToDate 比较 RPC 调用者的日志 与 当前节点日志哪个更新
// 返回 true: (lastLogTerm, lastLogIndex) 不比 当前节点的日志旧
// 必须在持有rf.mu时调用
func (rf *Raft) upToDate(lastLogIndex int, lastLogTerm int) bool {
	var (
		curLastLogIndex = rf.getGlobalIndex(len(rf.log) - 1)
		curLastLogTerm  = rf.log[len(rf.log)-1].Term
	)

	DPrintf(dLog, "S%v (*Raft).upToDate, lastLogIdx %v VS %v, lastLogTerm %v VS %v",
		[]interface{}{rf.me, curLastLogIndex, lastLogIndex, curLastLogTerm, lastLogTerm})

	if lastLogTerm != curLastLogTerm {
		// 优先比较任期, 任期大的日志一定更新
		return lastLogTerm > curLastLogTerm
	}

	// 任期相同比较日志长度
	return lastLogIndex >= curLastLogIndex
}

// logSyncer Leader 同步日志条目到指定 Follower
//  1. 每隔 15ms, 检查最新的日志条目是否发生了变化:
//     1.1 若发生了变化, 发起 AppendEntries RPC, 同步条目给 Follower
//     1.2 若没有发生变化, no-ops
//  2. 上一次 AppendEntries 到当前时间超过 150ms, 发起心跳
//
// 注: 若 raft 被 killed 或 任期发生变化, 退出 Goroutine
func (rf *Raft) logSyncer(server int, expectTerm int) {

	noOpsCnt := -1 // Leader 当选后立即发起一轮 AppendEntries RPC 作为心跳

	//tPrevEndLoop := time.Now().UnixMilli()
	for {
		//tBeginLoop := time.Now().UnixMilli()

		if rf.killed() {
			return
		}
		//tKill := time.Now().UnixMilli()

		rf.mu.Lock()

		//tLock := time.Now().UnixMilli()
		//DPrintf(dTest, LogCommonFormat+", logSyncer-> S%v, noOpsCnt %v, a new loop spend %v ms, rf.killed spend %v ms, acquire Lock spend %v ms,",
		//	[]interface{}{rf.me, rf.status.String(), rf.currentTerm, server, noOpsCnt,
		//		tBeginLoop - tPrevEndLoop, tKill - tBeginLoop, tLock - tKill})

		if rf.currentTerm != expectTerm {
			rf.mu.Unlock()
			return
		}

		curLastLogIndex := rf.getGlobalIndex(len(rf.log) - 1)

		if curLastLogIndex >= rf.nextIdx[server] || (noOpsCnt+1)%5 == 0 {
			// 日志发生变化 或 超出心跳周期, AppendEntries RPC 同步日志给 Follower
			DPrintf(dLog, LogCommonFormat+", logSyncer-> S%v, index %v vs %v, noOpsCnt %v",
				[]interface{}{rf.me, rf.status.String(), rf.currentTerm, server, curLastLogIndex, rf.nextIdx[server], noOpsCnt})
			rf.syncEntriesToFollower(server, expectTerm)
			noOpsCnt = 0
		} else {
			noOpsCnt++
		}
		rf.mu.Unlock()

		randSleepTime := GetRandom(int(HeartbeatPeriod/5-10*time.Millisecond), int(HeartbeatPeriod/5)) // 20ms~30ms
		time.Sleep(time.Duration(randSleepTime))

		//tPrevEndLoop = time.Now().UnixMilli()
	}
}

// syncEntriesToFollower 使用 AppendEntries 同步日志
// 注: sendAppendEntries 失败, 直接返回. logSyncer 中会以 15ms 为间隔重试
func (rf *Raft) syncEntriesToFollower(server int, expectTerm int) {

	for !rf.killed() && rf.currentTerm == expectTerm {

		lastLogIdx := rf.getGlobalIndex(len(rf.log) - 1)
		prevLogIdx := rf.nextIdx[server] - 1
		prevLogTerm := rf.log[rf.getLocalIndex(prevLogIdx)].Term
		entries := make([]*LogEntry, lastLogIdx-prevLogIdx)
		if len(entries) > 0 {
			copy(entries, rf.log[rf.getLocalIndex(prevLogIdx)+1:])
		}

		args := &AppendEntriesArgs{
			Term:         expectTerm,
			LeaderId:     rf.me,
			PrevLogIdx:   prevLogIdx,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIdx,
		}
		reply := &AppendEntriesReply{}

		rf.mu.Unlock()

		argStr, _ := sonic.MarshalString(args)
		DPrintf(dLog, LogCommonFormat+"->S%v, sendAppendEntries Args=%v", []interface{}{rf.me, NodeStateEnum_Leader, expectTerm, server, argStr})

		// rf.sendAppendEntries 需执行超时处理, 当网络断连时, 最高导致 logSyncer Goroutine 阻塞 7s
		timeout, complete := make(chan struct{}, 1), make(chan bool, 1)
		go startTimer(timeout, TimeScene_RPC)
		go func() {
			ok := rf.sendAppendEntries(server, args, reply)
			complete <- ok
		}()

		select {
		case <-timeout:
			DPrintf(dError, LogCommonFormat+"->S%v, [sendAppendEntries] timeout, retry", []interface{}{rf.me, NodeStateEnum_Leader, expectTerm, server})
			rf.mu.Lock()
			// 超时后立即重试
			continue
		case ok := <-complete:
			if !ok {
				// send rpc 报错, 返回
				DPrintf(dError, LogCommonFormat+" ->S%v, sendAppendEntries failed", []interface{}{rf.me, NodeStateEnum_Leader, expectTerm, server})
				rf.mu.Lock()
				return
			}
		}

		replyStr, _ := sonic.MarshalString(reply)
		DPrintf(dLog, LogCommonFormat+" ->S%v, sendAppendEntries reply=%v", []interface{}{rf.me, NodeStateEnum_Leader, expectTerm, server, replyStr})
		rf.mu.Lock()
		if rf.currentTerm < reply.Term {
			// 当前节点为过去任期, 切换为 Follower, 更新任期
			rf.follower(reply.Term, nil)
			return
		}

		if args.Term != rf.currentTerm {
			// 当前任期不等于发出请求时的任期, 不处理响应结果
			return
		}

		if reply.Success {
			// 更新nextIdx, matchIdx
			rf.nextIdx[server] = args.PrevLogIdx + len(args.Entries) + 1
			rf.matchIdx[server] = args.PrevLogIdx + len(args.Entries)
			DPrintf(dLog, LogCommonFormat+" ->S%v, syncEntriesToFollower Success=true, (matchIdx, nextIdx)=(%v, %v)",
				[]interface{}{rf.me, NodeStateEnum_Leader, rf.currentTerm, server, rf.matchIdx[server], rf.nextIdx[server]})
			return
		}

		// 更新 nextIdx 数组, 用于新一轮的 AppendEntries
		rf.nextIdx[server] = rf.calcNextIdx(reply.ConflictIdx, reply.ConflictTerm)
		DPrintf(dLog, LogCommonFormat+"->S%v, syncEntriesToFollower Success=false, nextIdx=%v", []interface{}{rf.me, NodeStateEnum_Leader, expectTerm, server, rf.nextIdx[server]})
	}
	return
}

// calcNextIdx 计算出 Follower 的 nextIdx
// case 1: conflictIdx=1, conflictTerm=2
//
//	0			1		 2
//
// Leader  	    T2		 T3
// Follower 	T2		 T2
//
// case 2: conflictIdx=2, conflictTerm=3
//
//	0			1		 2
//
// Leader  	    T2		 T2
// Follower 	T2		 T3
//
// case 3: conflictIdx=1, conflictTerm=nil
//
//	0			1		 2
//
// Leader  	    T2		 T2
// Follower 	T2
func (rf *Raft) calcNextIdx(conflictIdx int, conflictTerm *int) int {
	if conflictTerm == nil {
		// Follower 不包含 PrevLogIndex 索引的日志条目
		return conflictIdx + 1
	}

	// 寻找 Leader 中最后一个任期 conflictTerm 的日志条目
	nextIdx := conflictIdx
	for i := conflictIdx; i < rf.getGlobalIndex(len(rf.log)); i++ {
		if rf.log[rf.getLocalIndex(i)].Term == *conflictTerm {
			nextIdx++
		} else {
			break
		}
	}

	return nextIdx
}

func (rf *Raft) applier() {
	rf.mu.Lock()

	for {
		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		applyMsgs := make([]ApplyMsg, 0)
		for globalIdx := rf.lastApplied + 1; globalIdx <= rf.commitIdx; globalIdx++ {
			localIdx := rf.getLocalIndex(globalIdx)
			entry := rf.log[localIdx] // 数组越界了, 排查 rf.commitIdx、rf.log 更新
			applyMsgs = append(applyMsgs, ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: globalIdx,
			})
		}

		if len(applyMsgs) == 0 {
			// 等待新的日志提交
			rf.logCommit.Wait()
		} else {
			msgs, _ := sonic.MarshalString(applyMsgs)
			DPrintf(dApply, LogCommonFormat+", lastApplied %v->%v, applyMsgs: %v",
				[]interface{}{rf.me, rf.status, rf.currentTerm, rf.lastApplied, rf.lastApplied + len(applyMsgs), msgs})
			rf.lastApplied += len(applyMsgs)
			rf.mu.Unlock()
			for _, msg := range applyMsgs {
				rf.applyCh <- msg
			}
			rf.mu.Lock()
		}
	}
}

// 获取全局索引, 即 snapshot offset + localIndex
func (rf *Raft) getGlobalIndex(localIndex int) int {
	return rf.snapshotIdx + localIndex
}

func (rf *Raft) getLocalIndex(globalIndex int) int {
	return globalIndex - rf.snapshotIdx
}

// 二分查找, 找到 index 满足如下条件:
// 条件1. 超过 (rf.peers)/2 的成员的 rf.matchIdx >= index
// 条件2. 不存在比 index 大的索引满足条件 1
func (rf *Raft) maxIndexForCommit() int {

	left := rf.commitIdx
	right := rf.getGlobalIndex(len(rf.log) - 1)

	for left < right {
		mid := (left + right + 1) / 2
		cnt := 0
		for server, _ := range rf.peers {
			if rf.matchIdx[server] >= mid {
				cnt++
			}
		}

		if cnt > len(rf.peers)/2 {
			left = mid
		} else {
			right = mid - 1
		}
	}
	return left
}

func (rf *Raft) deleteEntryFrom(conflictIdx int) {
	if conflictIdx >= len(rf.log) {
		return
	}
	entries := make([]*LogEntry, conflictIdx)
	copy(entries, rf.log[:conflictIdx]) // for gc
	rf.log = entries
}
