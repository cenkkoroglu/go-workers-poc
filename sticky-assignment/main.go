package main

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"sync"
	"time"
)

type Task struct {
	ID           int
	SKU          string
	ResponseChan chan string
}

type Worker struct {
	ID             int
	Tasks          chan Task
	ProcessedTasks int
	// this part for able to see how many tasks that the worker processed, it is only for POC
	mu sync.Mutex
}

type Dispatcher struct {
	Workers []*Worker
}

type SKUInfo struct {
	WorkerID int
	LastUsed time.Time
}

func NewDispatcher(numWorkers int) *Dispatcher {
	d := &Dispatcher{
		Workers: make([]*Worker, numWorkers),
	}

	for i := 0; i < numWorkers; i++ {
		d.Workers[i] = &Worker{
			ID:    i,
			Tasks: make(chan Task, 100),
		}
		go d.Workers[i].Start()
	}

	return d
}

func (w *Worker) Start() {
	for task := range w.Tasks {
		// Simulate processing time
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
		fmt.Printf("Worker %d processed task %d with SKU %s\n", w.ID, task.ID, task.SKU)

		// this part for able to see how many tasks that the worker processed, it is only for POC
		w.mu.Lock()
		w.ProcessedTasks++
		w.mu.Unlock()
	}
}

func (d *Dispatcher) Dispatch(task Task) {
	h := fnv.New32a()
	_, _ = h.Write([]byte(task.SKU))
	workerID := int(h.Sum32()) % len(d.Workers)
	skuInfo := &SKUInfo{
		WorkerID: workerID,
		LastUsed: time.Now(),
	}
	d.Workers[skuInfo.WorkerID].Tasks <- task
}

func main() {
	numWorkers := 5
	dispatcher := NewDispatcher(numWorkers)

	// Simulate incoming tasks
	for i := 0; i < 10000; i++ {
		task := Task{
			ID:  i,
			SKU: fmt.Sprintf("SKU-%d", rand.Intn(100)),
		}
		dispatcher.Dispatch(task)
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond) // Simulate varying incoming task rate
	}

	// Wait for all tasks to be processed
	time.Sleep(10 * time.Second)

	// Print the number of tasks processed by each worker
	for _, worker := range dispatcher.Workers {
		fmt.Printf("Worker %d processed %d tasks\n", worker.ID, worker.ProcessedTasks)
	}
}
