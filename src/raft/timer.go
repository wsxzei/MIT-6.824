package raft

import (
	"log"
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
	case TimerScene_LogChecker:
		timeout = HeartbeatPeriod / 10
	case TimeScene_RPC:
		timeout = HeartbeatPeriod
	}
	time.Sleep(timeout)
	timeChan <- struct{}{}
}

func (rf *Raft) listenResetTimer(scene ResetTimerScene, resetTimer chan int, exit chan struct{}, expectTerm int) {
	DPrintf(dTimer, "S%v %v expectT%v, Start listening to reset timer event", []interface{}{rf.me, scene.String(), expectTerm})

	if resetTimer == nil || exit == nil || expectTerm < 0 {
		log.Panicf("S%v %v expectT%v, Invalid params", rf.me, scene.String(), expectTerm)
	}

	var rcvFrom *BufferIntChan
	switch scene {
	case ResetTimerScene_SelectionTimer:
		rcvFrom = rf.resetSelectionTimer
	case ResetTimerScene_Candidate:
		rcvFrom = rf.resetCandidate
	}
	if rcvFrom == nil {
		log.Panicf("S%v %v expectT%v, Invalid ResetTimerScene", rf.me, scene.String(), expectTerm)
	}
	rcvCh := rcvFrom.ReceiveAsync()

	// 两种情况下退出 for-loop:
	// 1. 成功发送事件任期到 resetTimer channel
	// 2. 从 exit channel 成功接收退出信息
	for {
		select {
		case eventTerm, ok := <-rcvCh:
			if !ok {
				DPrintf(dError, "S%v %v, expect T%v, event T%v, BufferIntChan.out is closed",
					[]interface{}{rf.me, scene.String(), expectTerm, eventTerm})
				rcvCh = nil
				continue
			}
			// 收到任期大于等于 expectTerm 时发送来的消息, 重置定时器
			if eventTerm >= expectTerm {
				select {
				// 收到期望任期的重置定时器事件, 向ticker发送信号
				case resetTimer <- eventTerm:
					DPrintf(dTimer, "S%v %v, expect T%v, event T%v, ResetTimer", []interface{}{rf.me, scene.String(), expectTerm, eventTerm})
				case <-exit:
					DPrintf(dTimer, "S%v %v, expect T%v, event T%v, exit ResetTimer Listener",
						[]interface{}{rf.me, scene.String(), expectTerm, eventTerm})
				}
				return
			} else {
				continue
			}
		case <-exit:
			DPrintf(dTimer, "S%v %v, expect T%v, exit ResetTimer Listener", []interface{}{rf.me, scene.String(), expectTerm})
			return
		}
	}
}
