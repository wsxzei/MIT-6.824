package raft

import (
	"math/rand"
	"time"
)

type NodeStateEnum int64

const (
	NodeStateEnum_Unset     NodeStateEnum = 0
	NodeStateEnum_Candidate NodeStateEnum = 1
	NodeStateEnum_Leader    NodeStateEnum = 2
	NodeStateEnum_Follower  NodeStateEnum = 3
)

const (
	MinSelectionTimeout = 450 * time.Millisecond
	MaxSelectionTimeout = 900 * time.Millisecond
	HeartbeatPeriod     = 150 * time.Millisecond
)

type TimerScene int64

const (
	TimerScene_Selection TimerScene = 1
	TImerScene_Heartbeat TimerScene = 2
)

type ResetTimerScene int64

const (
	ResetTimerScene_SelectionTimer ResetTimerScene = 1
	ResetTimerScene_Candidate      ResetTimerScene = 2
	ResetTimerScene_Leader         ResetTimerScene = 3
)

const (
	InitLogIndex = -1
	InitLogTerm  = -1
)

const (
	LogCommonFormat = "S%v %v T%v"
)

// GetRandom 获取[lowerBound, upperBound]范围的随机数
func GetRandom(lowerBound int, upperBound int) int {
	if lowerBound > upperBound {
		return GetRandom(upperBound, lowerBound)
	}

	randomTimeout := lowerBound
	delta := upperBound - lowerBound

	// 设置随机数种子
	rand.Seed(time.Now().UnixNano())

	// 生成[0, delta]之间的随机数
	randomTimeout += rand.Intn(delta + 1)
	return randomTimeout
}

// GenRandSelectionTimeout 生成随机的选举超时时间
func GenRandSelectionTimeout() time.Duration {
	timeoutNs := GetRandom(int(MinSelectionTimeout), int(MaxSelectionTimeout))
	return time.Duration(timeoutNs)
}

func (status NodeStateEnum) String() string {
	switch status {
	case NodeStateEnum_Candidate:
		return "Candidate"
	case NodeStateEnum_Leader:
		return "Leader"
	case NodeStateEnum_Follower:
		return "Follower"
	default:
		return "<Unset>"
	}
}

func (scene ResetTimerScene) String() string {
	switch scene {
	case ResetTimerScene_SelectionTimer:
		return "ResetSelectionTimer"
	case ResetTimerScene_Leader:
		return "ResetLeader"
	case ResetTimerScene_Candidate:
		return "ResetCandidate"
	default:
		return "<Unset>"
	}
}
