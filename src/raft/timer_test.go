package raft

import (
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestStartTimer(t *testing.T) {
	Convey("Test Timer", t, func() {

		for i := 0; i < 100; i++ {
			Convey(fmt.Sprintf("Test Timer for Heartbeat, %v", i), func() {

				begin := time.Now().UnixNano()
				timeout := make(chan struct{})
				go startTimer(timeout, TImerScene_Heartbeat)
				<-timeout
				end := time.Now().UnixNano()

				So(end-begin, ShouldBeLessThan, HeartbeatPeriod+2*time.Millisecond)
				So(end-begin, ShouldBeGreaterThan, HeartbeatPeriod-2*time.Millisecond)

			})
		}
		for i := 0; i < 100; i++ {
			Convey(fmt.Sprintf("Test Timer for Selection, %v", i), func() {
				begin := time.Now().UnixNano()
				timeout := make(chan struct{})
				go startTimer(timeout, TimerScene_Selection)
				<-timeout
				end := time.Now().UnixNano()

				So(end-begin, ShouldBeLessThan, MaxSelectionTimeout+5*time.Millisecond)
				So(end-begin, ShouldBeGreaterThan, MinSelectionTimeout-5*time.Millisecond)
			})
		}
	})
}

func TestListenResetTimer(t *testing.T) {

	Convey("Test listenResetTimer", t, func() {

		Convey("SelectionTimer", func() {
			rf := &Raft{
				resetSelectionTimer: NewBufferChan(make(chan int), make(chan int)),
			}
			go rf.resetSelectionTimer.Run()

			testTimes := 100

			go func() {
				i := 0
				// 只发送100个符合大于等于3的任期
				for i < testTimes {
					randTerm := GetRandom(0, 5) // 随机生成[0, 5]任期号
					if randTerm >= 3 {
						i++
					}
					go rf.resetSelectionTimer.Send(randTerm)
				}
			}()

			for i := 0; i < testTimes; i++ {
				resetTimer := make(chan int)
				exit := make(chan struct{})
				go rf.listenResetTimer(ResetTimerScene_SelectionTimer, resetTimer, exit, 3)

				select {
				case eventTerm := <-resetTimer:
					So(eventTerm, ShouldBeGreaterThanOrEqualTo, 3)
				}
			}

			rf.resetSelectionTimer.Kill()
		})

	})

}
