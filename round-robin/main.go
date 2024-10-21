package main

import (
	"fmt"
	"log"
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
			Tasks: make(chan Task, 1000),
		}
		go d.Workers[i].Start(&wg)
	}

	go d.startCleanupRoutine()

	return d
}

var wg sync.WaitGroup

func (w *Worker) Start(wg *sync.WaitGroup) {
	for task := range w.Tasks {
		fmt.Printf("Worker %d processed task %d with SKU %s\n", w.ID, task.ID, task.SKU)

		// Simulate task processing
		// time.Sleep(time.Duration(100) * time.Millisecond)

		// Mark task as done
		wg.Done()
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

var skuFrequencies = []struct {
	skuPrefix string
	weight    int // Higher weight means more frequent
}{
	{"SKU-42", 50}, // Most common SKU
	{"SKU-5", 30},  // Second most common SKU
	{"SKU-20", 20}, // Less common SKU
	{"SKU-7", 10},  // Even less common SKU
	{"random", 40}, // Random SKUs
}

// Function to generate SKU based on weighted probabilities
func generateSKU() string {
	// Calculate total weight
	totalWeight := 0
	for _, group := range skuFrequencies {
		totalWeight += group.weight
	}

	// Random number between 0 and totalWeight
	randVal := rand.Intn(totalWeight)

	// Pick the SKU group based on weight
	for _, group := range skuFrequencies {
		if randVal < group.weight {
			if group.skuPrefix == "random" {
				// Generate a fully random SKU
				return fmt.Sprintf("SKU-%d", rand.Intn(100))
			}
			return group.skuPrefix
		}
		randVal -= group.weight
	}

	return "SKU-default" // Fallback, though it shouldn't happen
}

func main() {
	rand.Seed(42)

	start := time.Now()

	numWorkers := 10
	dispatcher := NewDispatcher(numWorkers)

	// Simulate incoming tasks
	for i := 0; i < 1000000; i++ {
		task := Task{
			ID:  i,
			SKU: generateSKU(),
		}
		wg.Add(1)
		dispatcher.Dispatch(task)
	}

	// Wait for all tasks to be processed
	wg.Wait() // Wait for all tasks to be processed

	// Print the number of tasks processed by each worker
	for _, worker := range dispatcher.Workers {
		fmt.Printf("Worker %d processed %d tasks\n", worker.ID, worker.ProcessedTasks)
	}

	// Print the current size of the SKUAssignment map
	fmt.Printf("Current SKUAssignment map size: %d\n", len(dispatcher.SKUAssignment))

	elapsed := time.Since(start)
	log.Printf("Function took %s", elapsed)
}
