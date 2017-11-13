package main

import (
	//"fmt"
	//"io/ioutil"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	doneDir  = "./data/done/"
	newDir   = "./data/new/"
	workDir  = "./data/work/"
	errorDir = "./data/error/"
)

func worker(id int, jobs <-chan string, jobsDone *sync.WaitGroup) {
	log.Println("Worker:", id, "starting up")

	for {
		file, more := <-jobs
		if !more {
			log.Println("Worker:", id, "jobs closed")
			break
		}

		log.Println("Worker:", id, "working on", file)
		os.Rename(newDir+file, workDir+file)
		time.Sleep(2 * time.Second)

		log.Println("Worker:", id, "reading", file)
		cont, _ := ioutil.ReadFile(workDir + file)
		time.Sleep(2 * time.Second)

		if strings.Compare(strings.Trim(string(cont), "\n"), "error") == 0 {
			log.Println("Worker:", id, "error reading", file)
			os.Rename(workDir+file, errorDir+file)
		} else {
			log.Println("Worker:", id, "done with", file)
			os.Rename(workDir+file, doneDir+file)
		}
	}

	log.Println("Worker:", id, "shutting down")
	jobsDone.Done()
}

func jobGen(jobs chan<- string, done chan<- bool) {
	// setup signal watcher
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	log.Println("Job generator started")
	for {
		files, _ := ioutil.ReadDir(newDir)
		for _, file := range files {
			jobs <- file.Name()
		}

		select {
		case <-signals:
			log.Println("Directory watch got stop signal")
			done <- true
			return
		default:
			log.Println("Waiting for new files")
			time.Sleep(5 * time.Second)
		}
	}
}

func main() {
	jobs := make(chan string, 100)
	done := make(chan bool, 1)

	var workersDone sync.WaitGroup

	// setup 3 workers
	for w := 1; w <= 3; w++ {
		workersDone.Add(1)
		go worker(w, jobs, &workersDone)
	}

	// start watching the new directory
	go jobGen(jobs, done)

	// wait for the job generator to be done
	<-done
	close(jobs)

	// wait for all the workers
	workersDone.Wait()
}
