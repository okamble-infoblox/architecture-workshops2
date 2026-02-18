package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Batch represents a submitted batch of work items.
type Batch struct {
	ID        string      `json:"id"`
	Fast      int         `json:"fast"`
	Slow      int         `json:"slow"`
	StartedAt time.Time   `json:"started_at"`
	Results   []JobResult `json:"-"`
	Done      atomic.Int32
	Total     int `json:"total"`
	mu        sync.Mutex
}

// JobResult records completion of a single job.
type JobResult struct {
	Type     string        `json:"type"`
	Duration time.Duration `json:"duration"`
}

var (
	batches   = map[string]*Batch{}
	batchesMu sync.RWMutex
	batchSeq  atomic.Int64
)

// Run starts the worker service on :8081.
func Run() {
	port := envOr("WORKER_PORT", "8081")
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("POST /batches", handleSubmitBatch)
	mux.HandleFunc("GET /batches/{id}", handleBatchStatus)
	addr := ":" + port
	log.Printf("worker: listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("worker: %v", err)
	}
}

func handleSubmitBatch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Fast int `json:"fast"`
		Slow int `json:"slow"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Fast <= 0 && req.Slow <= 0 {
		http.Error(w, "must specify fast or slow > 0", http.StatusBadRequest)
		return
	}
	id := strconv.FormatInt(batchSeq.Add(1), 10)
	b := &Batch{
		ID:        id,
		Fast:      req.Fast,
		Slow:      req.Slow,
		Total:     req.Fast + req.Slow,
		StartedAt: time.Now(),
	}
	batchesMu.Lock()
	batches[id] = b
	batchesMu.Unlock()
	// LAB: STEP3 FIXED - Jobs now run in separate pools (bulkhead pattern).
	// Slow jobs are capped at 5 concurrent workers to prevent starvation.
	// Fast jobs have a larger pool (50 workers) for low latency processing.
	go processBatch(b)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"batch_id": id})
}

func processBatch(b *Batch) {
	// LAB: STEP3 FIXED - Implement bulkhead pattern with separate pools
	// Fast and slow jobs now use separate semaphores/pools to prevent starvation
	// Slow jobs are capped at 5 concurrent workers
	// Fast jobs can use up to 50 concurrent workers for low latency
	fastPoolSize := 50
	slowPoolSize := 5
	fastSem := make(chan struct{}, fastPoolSize)
	slowSem := make(chan struct{}, slowPoolSize)
	var wg sync.WaitGroup

	// Process fast jobs in their dedicated pool
	for i := 0; i < b.Fast; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fastSem <- struct{}{}
			defer func() { <-fastSem }()
			start := time.Now()
			time.Sleep(10 * time.Millisecond)
			b.recordResult("fast", time.Since(start))
		}()
	}

	// Process slow jobs in their dedicated pool with limited concurrency
	for i := 0; i < b.Slow; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			slowSem <- struct{}{}
			defer func() { <-slowSem }()
			start := time.Now()
			time.Sleep(1 * time.Second)
			b.recordResult("slow", time.Since(start))
		}()
	}
	wg.Wait()
}

func (b *Batch) recordResult(jobType string, d time.Duration) {
	b.mu.Lock()
	b.Results = append(b.Results, JobResult{Type: jobType, Duration: d})
	b.mu.Unlock()
	b.Done.Add(1)
}

func handleBatchStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	batchesMu.RLock()
	b, ok := batches[id]
	batchesMu.RUnlock()
	if !ok {
		http.Error(w, "batch not found", http.StatusNotFound)
		return
	}
	done := int(b.Done.Load())
	b.mu.Lock()
	results := make([]JobResult, len(b.Results))
	copy(results, b.Results)
	b.mu.Unlock()
	fastP95 := percentile(results, "fast", 0.95)
	slowP95 := percentile(results, "slow", 0.95)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"batch_id":    b.ID,
		"total":       b.Total,
		"done":        done,
		"complete":    done >= b.Total,
		"fast_p95_ms": fastP95,
		"slow_p95_ms": slowP95,
		"elapsed_ms":  time.Since(b.StartedAt).Milliseconds(),
	})
}

func percentile(results []JobResult, jobType string, pct float64) float64 {
	var durations []float64
	for _, r := range results {
		if r.Type == jobType {
			durations = append(durations, float64(r.Duration.Milliseconds()))
		}
	}
	if len(durations) == 0 {
		return 0
	}
	sort.Float64s(durations)
	idx := int(math.Ceil(pct*float64(len(durations)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(durations) {
		idx = len(durations) - 1
	}
	return durations[idx]
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
