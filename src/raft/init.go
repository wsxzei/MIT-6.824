package raft

import (
	"log"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	dClient       logTopic = "CLNT"
	dCommit       logTopic = "CMIT"
	dApply        logTopic = "Apply"
	dDrop         logTopic = "DROP"
	dError        logTopic = "ERRO"
	dInfo         logTopic = "INFO"
	dLeader       logTopic = "LEAD"
	dLog          logTopic = "LOG1"
	dLog2         logTopic = "LOG2"
	dPersist      logTopic = "PERS"
	dSnap         logTopic = "SNAP"
	dTerm         logTopic = "TERM"
	dTest         logTopic = "TEST"
	dTimer        logTopic = "TIMR"
	dHeartbeat    logTopic = "Heartbeat"
	dAppend       logTopic = "Append"
	dTrace        logTopic = "TRCE"
	dVote         logTopic = "VOTE"
	dWarn         logTopic = "WARN"
	dStatusSwitch logTopic = "StatusSwitch"
	dSelection    logTopic = "Selection"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime)) // 不使用log默认的日期和时间输出
}

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}
