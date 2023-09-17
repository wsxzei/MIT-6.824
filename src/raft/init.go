package raft

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

func init() {
	// Log as JSON instead of the default ASCII formatter.
	//log.SetFormatter(&log.JSONFormatter{})
	log.SetFormatter(&log.TextFormatter{
		TimestampFormat: "2006-01-02T15:04:05.999ms",
	})
	file, err := os.OpenFile(fmt.Sprintf("./debug/raft_%v.log", time.Now().UnixMilli()), os.O_CREATE|os.O_APPEND|os.O_WRONLY,
		0777)
	if err != nil {
		fmt.Printf("log init failed, os.OpenFile err=%v", err)
		panic("log init")
	}
	_, _ = fmt.Fprintf(file, "\n\n")

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(file)
}
