package main

import (
	"fmt"
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
	Workers       []*Worker
	SKUAssignment map[string]*SKUInfo
	mu            sync.Mutex
	roundRobinIdx int
}

type SKUInfo struct {
	WorkerID int
	LastUsed time.Time
}

const cleanupInterval = 30 * time.Second
const skuTTL = 10 * time.Minute

func NewDispatcher(numWorkers int) *Dispatcher {
	d := &Dispatcher{
		Workers:       make([]*Worker, numWorkers),
		SKUAssignment: make(map[string]*SKUInfo),
	}

	for i := 0; i < numWorkers; i++ {
		d.Workers[i] = &Worker{
			ID:    i,
			Tasks: make(chan Task, 100),
		}
		go d.Workers[i].Start()
	}

	go d.startCleanupRoutine()

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
	d.mu.Lock()
	defer d.mu.Unlock()

	skuInfo, exists := d.SKUAssignment[task.SKU]
	if !exists {
		// Assign new SKU to the worker with the least tasks
		workerID := d.getWorkerID()
		skuInfo = &SKUInfo{
			WorkerID: workerID,
			LastUsed: time.Now(),
		}
		d.SKUAssignment[task.SKU] = skuInfo
	} else {
		skuInfo.LastUsed = time.Now()
	}

	d.Workers[skuInfo.WorkerID].Tasks <- task
}

func (d *Dispatcher) getWorkerID() int {
	workerID := d.roundRobinIdx
	d.roundRobinIdx = (d.roundRobinIdx + 1) % len(d.Workers)
	return workerID
}

func (d *Dispatcher) startCleanupRoutine() {
	ticker := time.NewTicker(cleanupInterval)
	for range ticker.C {
		d.cleanupSKUAssignment()
	}
}

func (d *Dispatcher) cleanupSKUAssignment() {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	for sku, info := range d.SKUAssignment {
		if now.Sub(info.LastUsed) > skuTTL {
			delete(d.SKUAssignment, sku)
		}
	}
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

	// Print the current size of the SKUAssignment map
	fmt.Printf("Current SKUAssignment map size: %d\n", len(dispatcher.SKUAssignment))
}
