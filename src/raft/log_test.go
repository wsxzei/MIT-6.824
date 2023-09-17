package raft

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestUpToDate(t *testing.T) {
	Convey("Test log Uptodate", t, func() {
		// case1: 任期更大, 但索引更小
		Convey("Term is greater, index is less", func() {
			rf := &Raft{
				log: []*LogEntry{
					{
						Term: 1,
					},
					{
						Term: 3,
					},
				},
			}
			lastLogTerm, lastLogIndex := 4, 0
			isUptoDate := rf.upToDate(lastLogIndex, lastLogTerm)
			So(isUptoDate, ShouldEqual, true)
		})
		// case2: 任期更小, 但索引更大
		Convey("Term is less, index is greater", func() {
			rf := &Raft{
				log: []*LogEntry{
					{
						Term: 1,
					},
					{
						Term: 3,
					},
				},
			}
			lastLogTerm, lastLogIndex := 2, 4
			isUptoDate := rf.upToDate(lastLogIndex, lastLogTerm)
			So(isUptoDate, ShouldEqual, false)
		})

		// case3: 任期相同, 索引更大
		Convey("Term is equal, index is greater", func() {
			rf := &Raft{
				log: []*LogEntry{
					{
						Term: 1,
					},
					{
						Term: 3,
					},
				},
			}
			lastLogTerm, lastLogIndex := 3, 3
			isUptoDate := rf.upToDate(lastLogIndex, lastLogTerm)
			So(isUptoDate, ShouldEqual, true)
		})

		// 任期相同, 索引相同
		Convey("Term is equal, index is equal", func() {
			rf := &Raft{
				log: []*LogEntry{
					{
						Term: 1,
					},
					{
						Term: 3,
					},
				},
			}
			lastLogTerm, lastLogIndex := 3, 1
			isUptoDate := rf.upToDate(lastLogIndex, lastLogTerm)
			So(isUptoDate, ShouldEqual, true)
		})

		// 任期相同, 索引更小
		Convey("Term is equal, index is less", func() {
			rf := &Raft{
				log: []*LogEntry{
					{
						Term: 1,
					},
					{
						Term: 3,
					},
					{
						Term: 3,
					},
				},
			}
			lastLogTerm, lastLogIndex := 3, 1
			isUptoDate := rf.upToDate(lastLogIndex, lastLogTerm)
			So(isUptoDate, ShouldEqual, false)
		})

	})
}
