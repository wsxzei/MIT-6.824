package raft

import (
	log "github.com/sirupsen/logrus"
	"time"
)

func startTimer(timeChan chan struct{}, timerScene TimerScene) {
	if timeChan == nil {
		log.Panicf("[startTimer] timeChan is nil")
	}
	var timeout time.Duration
	switch timerScene {
	case TimerScene_Selection:
		timeout = GenRandSelectionTimeout()
	case TImerScene_Heartbeat:
		timeout = HeartbeatPeriod
	}
	time.Sleep(timeout)
	timeChan <- struct{}{}
}

func (rf *Raft) listenResetTimer(scene ResetTimerScene, resetTimer chan int, exit chan struct{}, expectTerm int) {
	logCtx := log.WithFields(log.Fields{
		"method":          "(*Raft).listenResetTimer",
		"me":              rf.me,
		"expectTerm":      expectTerm,
		"resetTimerScene": scene.String(),
	})

	if resetTimer == nil || exit == nil || expectTerm < 0 {
		logCtx.Panicf("invalid params")
	}

	var rcvFrom *BufferIntChan
	switch scene {
	case ResetTimerScene_SelectionTimer:
		rcvFrom = rf.resetSelectionTimer
	case ResetTimerScene_Candidate:
		rcvFrom = rf.resetCandidate
	case ResetTimerScene_Leader:
		rcvFrom = rf.resetLeader
	}
	if rcvFrom == nil {
		logCtx.Panic("Invalid ResetTimerScene")
	}

	for !rf.killed() {
		select {
		case eventTerm, ok := <-rcvFrom.ReceiveAsync():
			logCtx = logCtx.WithFields(log.Fields{"eventTerm": eventTerm})
			if !ok {
				logCtx.Error("BufferIntChan.out is closed")
				return
			}
			// 收到任期大于等于 expectTerm 时发送来的消息, 重置定时器
			if eventTerm >= expectTerm {
				select {
				// 收到期望任期的重置定时器事件, 向ticker发送信号
				case resetTimer <- eventTerm:
					logCtx.Info("Reset Timer!")
				case <-exit:
					logCtx.Info("Goroutine exit")
				}
				return
			} else {
				logCtx.Info("Skip out of date term")
				continue
			}
		case <-exit:
			logCtx.Info("Goroutine exit")
			return
		}
	}
}
