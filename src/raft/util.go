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
