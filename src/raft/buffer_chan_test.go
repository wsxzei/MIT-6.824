package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestBufferChan(t *testing.T) {
	res := make([]bool, 501)
	in, out := make(chan int), make(chan int)
	bufferChan := NewBufferChan(in, out)

	go bufferChan.Run()

	rand.Seed(time.Now().UnixNano())
	waitSender, waitRcver := sync.WaitGroup{}, sync.WaitGroup{}
	for i := 1; i <= 10; i++ {
		waitSender.Add(1)
		go func(i int) {
			defer waitSender.Done()
			for j := 1; j <= 50; j++ {
				// 30~50ms
				time.Sleep(time.Duration(rand.Intn(21)+30) * time.Millisecond)
				bufferChan.Send((i-1)*50 + j)
			}
		}(i)
	}

	waitRcver.Add(1)
	go func() {
		defer waitRcver.Done()
		for {
			i, ok := bufferChan.Receive()
			if ok {
				res[i] = true
			} else {
				return
			}
		}
	}()

	waitSender.Wait()
	bufferChan.Close()

	waitRcver.Wait()

	// 目标是打印所有1~500的整数
	for i := 1; i <= 500; i++ {
		if !res[i] {
			fmt.Printf("Not Pass Test, i=%v", i)
			return
		}
	}
	fmt.Println("Pass Test")
}
