package raft

import log "github.com/sirupsen/logrus"

// upToDate 比较 RPC 调用者的日志 与 当前节点日志哪个更新
// 返回 true: (lastLogTerm, lastLogIndex) 不比 当前节点的日志旧
// 必须在持有rf.mu时调用
func (rf *Raft) upToDate(lastLogIndex int, lastLogTerm int) bool {
	var (
		curLastLogIndex = InitLogIndex
		curLastLogTerm  = InitLogTerm
	)

	length := len(rf.log)
	if length > 0 {
		curLastLogIndex = length - 1
		curLastLogTerm = rf.log[curLastLogIndex].Term
	}

	log.WithFields(log.Fields{
		"method":          "(*Raft).upToDate",
		"curLastLogIndex": curLastLogIndex,
		"reqLastLogIndex": lastLogIndex,
		"curLastLogTerm":  curLastLogTerm,
		"reqLastLogTerm":  lastLogTerm,
	}).Info()
	if lastLogTerm != curLastLogTerm {
		// 优先比较任期, 任期大的日志一定更新
		return lastLogTerm > curLastLogTerm
	}

	// 任期相同比较日志长度
	return lastLogIndex >= curLastLogIndex
}
