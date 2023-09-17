package log

import (
	log "github.com/sirupsen/logrus"
	"sync"
	"testing"
	"time"
)

func TestLog(t *testing.T) {
	log.WithFields(log.Fields{
		"animal": "walrus",
		"size":   10,
	}).Info("A group of walrus emerges from the ocean")

	log.WithFields(log.Fields{
		"omg":    true,
		"number": 122,
	}).Warn("The group's number increased tremendously!")

	time.Sleep(40 * time.Millisecond)
	log.WithFields(log.Fields{
		"name": "wzz",
	}).Error("Test Error Level log")
	//log.WithFields(log.Fields{
	//	"omg":    true,
	//	"number": 100,
	//}).Fatal("The ice breaks!")

	// A common pattern is to re-use fields between logging statements by re-using
	// the logrus.Entry returned from WithFields()
	contextLogger := log.WithFields(log.Fields{
		"common": "this is a common field",
		"other":  "I also should be logged always",
	})

	contextLogger.Info("I'll be logged with common and other field")
	contextLogger.Info("Me too")

}

func TestLogConcurrent(t *testing.T) {
	logCtx := log.WithFields(log.Fields{
		"name": "wzz",
		"age":  21,
	})
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			logCtx.WithFields(log.Fields{
				"idx": idx,
			}).Info("Running!")
		}(i)
	}
	wg.Wait()
}
