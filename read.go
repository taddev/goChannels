package main

import (
	//"fmt"
	//"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	doneDir  string = "./data/done/"
	newDir   string = "./data/new/"
	workDir  string = "./data/work/"
	errorDir string = "./data/error/"
)

func worker(id int, jobs <-chan int, results chan<- int, jobsDone chan<- bool) {
	log.Println("Worker:", id, "starting up")

	for j := range jobs {
		log.Println("Worker:", id, "working on job", j)
		time.Sleep(5 * time.Second)
		results <- j * id
	}

	log.Println("Worker:", id, "shutting down")
	jobsDone <- true
}

func jobGen(jobs chan<- int, stop <-chan bool, done chan<- bool) {
	j := 1
	log.Println("Job generator started")
	for {
		jobs <- j
		j++

		select {
		case <-stop:
			log.Println("Job generator got stop signal")
			done <- true
			return
		default:
			time.Sleep(5 * time.Second)
		}

	}
	log.Println("Job generator stopped")
}

func resultsProc(results <-chan int, finished chan<- bool) {
	log.Println("Waiting for results")
	for r := range results {
		log.Println("Result:", r)
	}

	log.Println("Results worker done")
	finished <- true
}

func signalHandle(signals chan os.Signal, stop chan<- bool) {
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// wait for a signal
	<-signals
	log.Println()
	log.Println("Got signal, sending all stop")
	stop <- true
}

func main() {
	jobs := make(chan int, 100)
	results := make(chan int, 100)
	signals := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	stop := make(chan bool, 1)
	jobsDone := make(chan bool, 3)
	finished := make(chan bool, 1)

	// setup 3 workers
	for w := 1; w <= 3; w++ {
		go worker(w, jobs, results, jobsDone)
	}

	// read the results
	go resultsProc(results, finished)

	// start generating jobs
	go jobGen(jobs, stop, done)

	// wait for termination signal
	go signalHandle(signals, stop)

	// wait for the job generator to be done
	<-done
	close(jobs)

	// wait for all the workers
	for j := 1; j <= 3; j++ {
		<-jobsDone
	}
	close(results)

	// wait for the results to finish
	<-finished
}
