package raft

import (
	"github.com/bytedance/sonic"
	"time"
)

// upToDate compare the last entry's index and term of RequestVote RPC sender with
// current Raft instance. If return true, (lastLogIndex, lastLogTerm) is uptodate.
// note: rf.upToDate needs to be called with the protection of rf.mu
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

// logSyncer Leader synchronize log entries to specific Follower
//  1. Every once in a while (I choose 20~30ms), determine whether there is some new log entries.
//     1.1 if yes, send AppendEntries RPC to Follower.
//     1.2 if no, just increment no-ops count.
//  2. If no-ops count is equal to -1 or 4, which means current Raft instance just become leader
//     or a heartbeat period is elapsed, send heartbeat.
//
// note: if raft instance is killed or currentTerm has changed, exit logSyncer Goroutine.
func (rf *Raft) logSyncer(server int, expectTerm int) {
	noOpsCnt := -1

	for !rf.killed() {
		rf.Lock("[rf.logSyncer] begin a new for-loop")

		if rf.currentTerm != expectTerm {
			rf.Unlock("[rf.logSyncer] rf.currentTerm changed, exit logSyncer")
			return
		}

		curLastLogIndex := rf.getGlobalIndex(len(rf.log) - 1)

		if curLastLogIndex >= rf.nextIdx[server] || (noOpsCnt+1)%5 == 0 {
			DPrintf(dLog, LogCommonFormat+", logSyncer-> S%v, index %v vs %v, noOpsCnt %v",
				[]interface{}{rf.me, rf.status.String(), rf.currentTerm, server, curLastLogIndex, rf.nextIdx[server], noOpsCnt})
			rf.syncEntriesToFollower(server, expectTerm)
			noOpsCnt = 0
		} else {
			noOpsCnt++
		}
		rf.Unlock("[rf.logSyncer] before Goroutine Sleep")

		randSleepTime := GetRandom(int(HeartbeatPeriod/5-10*time.Millisecond), int(HeartbeatPeriod/5)) // 20ms~30ms
		time.Sleep(time.Duration(randSleepTime))
	}
}

// syncEntriesToFollower send multiple rounds of AppendEntries RPC to synchronize log to Follower
func (rf *Raft) syncEntriesToFollower(server int, expectTerm int) {

	for !rf.killed() && rf.currentTerm == expectTerm {

		lastLogIdx := rf.getGlobalIndex(len(rf.log) - 1)
		prevLogIdx := rf.nextIdx[server] - 1

		// if prevLogIndex is less than rf.snapshotIdx, send InstallSnapshot RPC
		if prevLogIdx < rf.snapshotIdx {
			if retry := rf.syncBySnapshot(server); retry {
				// send rpc timeout, immediately retry
				continue
			}
			return
		}

		prevLogTerm := rf.log[rf.getLocalIndex(prevLogIdx)].Term
		entries := make([]*LogEntry, lastLogIdx-prevLogIdx)
		// notes: The following copy method will cause bugs,
		// because I expose the reference of LogEntry object without the protection of rf.mu
		// and in method rf.deleteTo, I will initialize dummy Command to nil.
		// There are two solutions:
		// 1. deep copy LogEntry Object
		// 2. Once generate LogEntry Object, don't modify the fields in it
		//
		//if len(entries) > 0 {
		//	copy(entries, rf.log[rf.getLocalIndex(prevLogIdx)+1:])
		//}
		j := 0
		for i := rf.getLocalIndex(prevLogIdx) + 1; i < len(rf.log); i++ {
			entries[j] = &LogEntry{
				Term:    rf.log[i].Term,
				Command: rf.log[i].Command,
			}
			j++
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

		rf.Unlock("[rf.syncEntriesToFollower] before select")

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
			DPrintf(dError, LogCommonFormat+" ->S%v, sendAppendEntries timeout, retry", []interface{}{rf.me, NodeStateEnum_Leader, expectTerm, server})
			rf.Lock("[rf.syncEntriesToFollower] rf.sendAppendEntries timeout")
			// Once rpc timeout, retry immediately
			continue
		case ok := <-complete:
			rf.Lock("[rf.syncEntriesToFollower] rf.sendAppendEntries complete")
			if !ok {
				DPrintf(dError, LogCommonFormat+" ->S%v, sendAppendEntries failed", []interface{}{rf.me, NodeStateEnum_Leader, expectTerm, server})
				return
			}
		}

		replyStr, _ := sonic.MarshalString(reply)
		DPrintf(dLog, LogCommonFormat+" ->S%v, sendAppendEntries reply=%v", []interface{}{rf.me, NodeStateEnum_Leader, expectTerm, server, replyStr})
		if rf.currentTerm < reply.Term {
			// currentTerm is less than receiver, switch to follower and update currentTerm
			rf.follower(reply.Term, nil)
			return
		}

		if args.Term != rf.currentTerm {
			// currentTerm isn't equal to the term when raft send RPC, don't handle reply
			return
		}

		if reply.Success {
			// Once Follower passes consistency check, update Leader's nextIndex and matchIndex
			rf.nextIdx[server] = args.PrevLogIdx + len(args.Entries) + 1
			rf.matchIdx[server] = args.PrevLogIdx + len(args.Entries)
			DPrintf(dLog, LogCommonFormat+" ->S%v, syncEntriesToFollower Success=true, (matchIdx, nextIdx)=(%v, %v)",
				[]interface{}{rf.me, NodeStateEnum_Leader, rf.currentTerm, server, rf.matchIdx[server], rf.nextIdx[server]})
			return
		}

		// If this round of AppendEntries RPC failed, decrement nextIndex and retry
		rf.nextIdx[server] = rf.calcNextIdx(reply.ConflictIdx, reply.ConflictTerm)
		DPrintf(dLog, LogCommonFormat+" ->S%v, syncEntriesToFollower Success=false, nextIdx=%v", []interface{}{rf.me, NodeStateEnum_Leader, expectTerm, server, rf.nextIdx[server]})
	}
	return
}

func (rf *Raft) syncBySnapshot(server int) (retry bool) {
	snapshot := rf.persister.snapshot

	args := &InstallSnapshotArgs{
		Term:      rf.currentTerm,
		LeaderId:  rf.me,
		LastIndex: rf.snapshotIdx,
		LastTerm:  rf.log[0].Term,
		Snapshot:  snapshot,
	}
	reply := &InstallSnapshotReply{}

	rf.Unlock("[rf.syncBySnapshot] before select")

	argStr, _ := sonic.MarshalString(args)
	DPrintf(dSnap, LogCommonFormat+" ->S%v, InstallSnapshotArgs = %v",
		[]interface{}{rf.me, rf.status, rf.currentTerm, server, argStr})

	timeout, complete := make(chan struct{}, 1), make(chan bool, 1)
	go startTimer(timeout, TimeScene_RPC)

	go func() {
		ok := rf.sendInstallSnapshot(server, args, reply)
		complete <- ok
	}()

	select {
	case <-timeout:
		rf.Lock("[rf.syncBySnapshot] sendInstallSnapshot timeout")
		DPrintf(dSnap, LogCommonFormat+" ->S%v, sendInstallSnapshot timeout", []interface{}{rf.me, rf.status, rf.currentTerm, server})
		retry = true
		return
	case ok := <-complete:
		rf.Lock("[rf.syncBySnapshot] sendInstallSnapshot complete")
		if !ok {
			DPrintf(dSnap, LogCommonFormat+" ->S%v, sendInstallSnapshot failed", []interface{}{rf.me, rf.status, rf.currentTerm, server})
			return
		}
	}

	DPrintf(dSnap, LogCommonFormat+" ->S%v, sendInstallSnapshot reply.Term %v", []interface{}{rf.me, rf.status, rf.currentTerm, server, reply.Term})
	if rf.currentTerm < reply.Term {
		rf.follower(reply.Term, nil)
		return
	}

	if rf.currentTerm != args.Term {
		return
	}

	// Leader receives the reply of InstallSnapshot RPC, which doesn't mean that
	// the Followers has installed snapshot.
	// But it doesn't matter to update (nextIdx, matchIdx) --> (LastIndex + 1, LastIndex).
	// Even if the installing snapshot fails, the updating of nextIdx will result in at most one redundant AppendEntries RPC,
	// and the updating of matchIdx won't affect the commit of the log entries after index LastIndex.
	// note: don't use rf.getGlobalIndex, rf.snapshotIdx may be updated by rf.Snapshot
	rf.nextIdx[server] = args.LastIndex + 1
	rf.matchIdx[server] = args.LastIndex
	DPrintf(dSnap, LogCommonFormat+" ->S%v, syncBySnapshot update (matchIdx, nextIdx)->(%v, %v)",
		[]interface{}{rf.me, rf.status, rf.currentTerm, server, rf.matchIdx[server], rf.nextIdx[server]})
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

	// find the last log entry whose term is conflictTerm in Leader's log
	// if conflictIdx isn't contained in the log but in the snapshot,
	// set nextIdx to snapshotIndex,
	// so that if the entry term for index snapshotIndex is not equal to conflictTerm,
	// an InstallSnapshot RPC will be sent
	nextIdx := MaxInt(conflictIdx, rf.snapshotIdx)

	for i := nextIdx; i < rf.getGlobalIndex(len(rf.log)); i++ {
		if rf.log[rf.getLocalIndex(i)].Term == *conflictTerm {
			nextIdx++
		} else {
			break
		}
	}

	return nextIdx
}

func (rf *Raft) applier() {
	rf.Lock("[rf.applier] start applier Goroutine")

	for {
		if rf.killed() {
			rf.Unlock("[rf.applier] rf is killed, applier exit")
			return
		}

		applyMsgs := make([]ApplyMsg, 0)
		// rf.lastApplied must greater than or equal to rf.snapshotIdx
		for globalIdx := rf.lastApplied + 1; globalIdx <= rf.commitIdx; globalIdx++ {
			localIdx := rf.getLocalIndex(globalIdx)
			entry := rf.log[localIdx]
			applyMsgs = append(applyMsgs, ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: globalIdx,
			})
		}

		if len(applyMsgs) == 0 {
			// wait for some new log entries committed
			rf.logCommitWait("[rf.applier] applyMsgs is empty")
		} else {
			msgs, _ := sonic.MarshalString(applyMsgs)
			DPrintf(dApply, LogCommonFormat+", lastApplied %v->%v, applyMsgs: %v",
				[]interface{}{rf.me, rf.status, rf.currentTerm, rf.lastApplied, rf.lastApplied + len(applyMsgs), msgs})
			rf.lastApplied += len(applyMsgs)
			rf.Unlock("[rf.applier] begin sending applyMsgs")
			for _, msg := range applyMsgs {
				rf.applyCh <- msg
			}
			rf.Lock("[rf.applier] complete sending applyMsgs")
		}
	}
}

// GlobalIndex: snapshot offset + localIndex
func (rf *Raft) getGlobalIndex(localIndex int) int {
	return rf.snapshotIdx + localIndex
}

func (rf *Raft) getLocalIndex(globalIndex int) int {
	return globalIndex - rf.snapshotIdx
}

// maxIndexForCommit binary search the 'index' which demands the following conditions:
// Condition 1: a majority of cluster peers' matchIndex are more than 'index'
// Condition 2: There is no index lager than  'index' which demands Condition 1.
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

// delete local log entries to index (including)
func (rf *Raft) deleteTo(index int) {
	before, _ := sonic.MarshalString(rf.log)
	lastEntryIndex := rf.getGlobalIndex(len(rf.log) - 1)
	if index > lastEntryIndex {
		index = lastEntryIndex
	}

	newSize := lastEntryIndex - index + 1 // include dummy Entry
	entries := make([]*LogEntry, newSize)
	copy(entries, rf.log[rf.getLocalIndex(index):])
	entries[0].Command = nil // just maintain snapshotTerm in the dummy entry

	rf.log = entries
	after, _ := sonic.MarshalString(rf.log)
	DPrintf(dLog, LogCommonFormat+" snapshotIdx %v, rf.deleteTo(I%v), log: %v\n--> %v",
		[]interface{}{rf.me, rf.status, rf.currentTerm, rf.snapshotIdx, index, before, after})
}
