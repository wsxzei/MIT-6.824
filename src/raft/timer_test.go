package raft

import (
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"sync"
	"testing"
	"time"
)

func TestStartTimer(t *testing.T) {
	Convey("Test Timer", t, func() {

		for i := 0; i < 100; i++ {
			Convey(fmt.Sprintf("Test Timer for LogChecker, %v", i), func() {

				begin := time.Now().UnixNano()
				timeout := make(chan struct{})
				go startTimer(timeout, TimerScene_LogChecker)
				<-timeout
				end := time.Now().UnixNano()

				So(end-begin, ShouldBeLessThan, HeartbeatPeriod/10+2*time.Millisecond)
				So(end-begin, ShouldBeGreaterThan, HeartbeatPeriod/10-2*time.Millisecond)
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

		//Convey("SelectionTimer", func() {
		//	rf := &Raft{
		//		resetSelectionTimer: NewBufferChan(make(chan int), make(chan int)),
		//	}
		//	go rf.resetSelectionTimer.Run()
		//
		//	testTimes := 100
		//
		//	go func() {
		//		i := 0
		//		// 只发送100个符合大于等于3的任期
		//		for i < testTimes {
		//			randTerm := GetRandom(0, 5) // 随机生成[0, 5]任期号
		//			if randTerm >= 3 {
		//				i++
		//			}
		//			go rf.resetSelectionTimer.Send(randTerm)
		//		}
		//	}()
		//
		//	for i := 0; i < testTimes; i++ {
		//		resetTimer := make(chan int)
		//		exit := make(chan struct{})
		//		go rf.listenResetTimer(ResetTimerScene_SelectionTimer, resetTimer, exit, 3)
		//
		//		select {
		//		case eventTerm := <-resetTimer:
		//			So(eventTerm, ShouldBeGreaterThanOrEqualTo, 3)
		//		}
		//	}
		//
		//})

		Convey("Test BufferChan exit", func() {
			rf := &Raft{
				resetSelectionTimer: NewBufferChan(make(chan int), make(chan int)),
			}
			go rf.resetSelectionTimer.Run()

			wg := sync.WaitGroup{}
			wg.Add(2)

			// 测试 rf.listenResetTimer 退出
			resetTimer := make(chan int)
			exit := make(chan struct{})

			go func() {
				defer wg.Done()
				time.Sleep(100 * time.Millisecond)
				rf.resetSelectionTimer.Kill() // 关闭 BufferChan
				time.Sleep(100 * time.Millisecond)
				exit <- struct{}{}
			}()

			go func() {
				defer wg.Done()
				rf.listenResetTimer(ResetTimerScene_SelectionTimer, resetTimer, exit, 3)
			}()

			wg.Wait()
		})

	})

}
