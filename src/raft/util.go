package raft

import (
	"fmt"
	"log"
	"time"
)

// DPrintf 打印可解析的日志
// eg: 008259 VOTE S1 <- S2 Got vote
func DPrintf(topic logTopic, format string, a []interface{}) {
	if debugVerbosity >= 1 {
		time_ := time.Since(debugStart).Microseconds()          // 显示相对时间
		time_ /= 100                                            // 显示精度0.1 ms
		prefix := fmt.Sprintf("%06d %v ", time_, string(topic)) // 相对时间 + 主题
		format = prefix + format
		log.Printf(format, a...)
	}
}

func NewInt(i int) *int {
	retInt := new(int)
	*retInt = i
	return retInt
}

func IntPtrToVal(ptr *int, defaultVal int) int {
	if ptr != nil {
		return *ptr
	}
	return defaultVal
}

func MinInt(i1 int, i2 int) int {
	if i1 < i2 {
		return i1
	}
	return i2
}

func MaxInt(i1 int, i2 int) int {
	if i1 < i2 {
		return i2
	}
	return i1
}

func (rf *Raft) Lock(msg string) {
	rf.mu.Lock()
	if getVerbosity() > 2 {
		DPrintf(dTest, "S%v acquire Lock, %v T%v, msg %v",
			[]interface{}{rf.me, rf.status, rf.currentTerm, msg})
	}
}

func (rf *Raft) Unlock(msg string) {
	if getVerbosity() > 2 {
		DPrintf(dTest, "S%v release Lock, %v T%v, msg %v",
			[]interface{}{rf.me, rf.status, rf.currentTerm, msg})
	}
	rf.mu.Unlock()
}

func (rf *Raft) toFollowerWait(msg string) {
	if getVerbosity() > 2 {
		DPrintf(dTest, "S%v release Lock, %v T%v, msg %v",
			[]interface{}{rf.me, rf.status, rf.currentTerm, msg})
	}
	rf.toFollower.Wait()
	if getVerbosity() > 2 {
		DPrintf(dTest, "S%v acquire Lock, %v T%v, msg %v",
			[]interface{}{rf.me, rf.status, rf.currentTerm, msg})
	}
}

func (rf *Raft) logCommitWait(msg string) {
	if getVerbosity() > 2 {
		DPrintf(dTest, "S%v release Lock, %v T%v, msg %v",
			[]interface{}{rf.me, rf.status, rf.currentTerm, msg})
	}
	rf.logCommit.Wait()
	if getVerbosity() > 2 {
		DPrintf(dTest, "S%v acquire Lock, %v T%v, msg %v",
			[]interface{}{rf.me, rf.status, rf.currentTerm, msg})
	}
}