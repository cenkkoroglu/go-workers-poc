package main

import (
	"fmt"
	"hash/fnv"
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
			Tasks: make(chan Task, 1000),
		}
		go d.Workers[i].Start(&wg)
	}

	return d
}

var wg sync.WaitGroup

func (w *Worker) Start(wg *sync.WaitGroup) {
	for task := range w.Tasks {
		fmt.Printf("Worker %d processed task %d with SKU %s\n", w.ID, task.ID, task.SKU)

		// Simulate task processing
		time.Sleep(time.Duration(50) * time.Millisecond)

		// Mark task as done
		wg.Done()
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

	elapsed := time.Since(start)
	log.Printf("Function took %s", elapsed)
}
