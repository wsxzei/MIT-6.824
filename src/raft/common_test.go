package raft

import (
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"log"
	"testing"
	"time"
)

func TestGetRandomTimeout(t *testing.T) {
	Convey("TestGetRandomTimeout", t, func() {
		for i := 0; i < 200; i++ {
			timeout := GenRandSelectionTimeout()
			So(timeout, ShouldBeLessThan, MaxSelectionTimeout)
			So(timeout, ShouldBeGreaterThan, MinSelectionTimeout)
		}
	})

}

func TestTimer(t *testing.T) {
	timer := time.NewTimer(GenRandSelectionTimeout())
	done := make(chan bool, 50)
	exit := make(chan bool)
	go func() {
		ticker := time.NewTicker(750 * time.Millisecond)
		for {
			select {
			case <-exit:
				log.Println("Ticker Exit")
				return
			case <-ticker.C:
				log.Println("Tick Tick Tick, Task done")
				done <- true
			}
		}
	}()
	var i int
	for i < 50 {
		nextTimeout := GenRandSelectionTimeout()
		So(nextTimeout, ShouldBeLessThan, MaxSelectionTimeout)
		So(nextTimeout, ShouldBeGreaterThan, MinSelectionTimeout)
		select {
		case <-timer.C:

			log.Printf("timer is timeout, nextTimeout=%v\n", nextTimeout)
			timer.Reset(nextTimeout)
		case <-done:
			log.Printf("task is finished, nextTimeout = %v\n", nextTimeout)
			timer.Reset(nextTimeout)
			i++
		}
	}
	exit <- true
}

func TestSetLength(t *testing.T) {
	set := make(map[int]struct{})

	for i := 0; i < 50; i++ {
		set[i] = struct{}{}
	}

	fmt.Println(len(set))

	ptrSet := make(map[*int]struct{})
	for i := 1; i <= 50; i++ {
		ptrSet[NewInt(i)] = struct{}{}
	}
	fmt.Println(len(ptrSet))
	ptrSet[nil] = struct{}{}
	fmt.Println(len(ptrSet))
}
