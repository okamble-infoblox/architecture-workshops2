package driver

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/infobloxopen/architecture-workshops2/pkg/report"
)

// Runner executes a load test against a target URL.
type Runner struct {
	Config  RunConfig
	Results []RequestResult
	mu      sync.Mutex
}

// RunConfig configures a load test run.
type RunConfig struct {
	TargetURL   string
	Method      string
	Body        string
	RPS         int
	Duration    time.Duration
	Concurrency int
}

// RequestResult records the outcome of a single request.
type RequestResult struct {
	StatusCode int
	Latency    time.Duration
	Error      error
	Timestamp  time.Time
}

// NewRunner creates a Runner with the given config.
func NewRunner(cfg RunConfig) *Runner {
	if cfg.Method == "" {
		cfg.Method = "GET"
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = cfg.RPS
	}
	return &Runner{Config: cfg}
}

// Run executes the load test and returns collected metrics.
func (r *Runner) Run(ctx context.Context) *report.RunData {
	startedAt := time.Now()
	runID := fmt.Sprintf("%s-%d", startedAt.Format("20060102-150405"), rand.Intn(1000))

	ctx, cancel := context.WithTimeout(ctx, r.Config.Duration)
	defer cancel()

	sem := make(chan struct{}, r.Config.Concurrency)
	ticker := time.NewTicker(time.Second / time.Duration(r.Config.RPS))
	defer ticker.Stop()

	var wg sync.WaitGroup
	var sent atomic.Int64

	client := &http.Client{Timeout: 30 * time.Second}

	tsInterval := time.Second
	tsTicker := time.NewTicker(tsInterval)
	defer tsTicker.Stop()
	var timeseries []report.TimeseriesDP
	var tsLastCount int64
	var tsLastErrors int64
	tsStart := time.Now()

	var totalErrors atomic.Int64

	tsDone := make(chan struct{})
	go func() {
		defer close(tsDone)
		for {
			select {
			case <-tsTicker.C:
				elapsed := time.Since(tsStart).Seconds()
				currentTotal := sent.Load()
				currentErrors := totalErrors.Load()
				intervalReqs := float64(currentTotal - tsLastCount)
				intervalErrs := float64(currentErrors - tsLastErrors)
				errRate := 0.0
				if intervalReqs > 0 {
					errRate = intervalErrs / intervalReqs
				}
				p95 := r.recentP95()
				timeseries = append(timeseries, report.TimeseriesDP{
					Elapsed:    elapsed,
					RPS:        intervalReqs,
					LatencyP95: p95,
					ErrorRate:  errRate,
				})
				tsLastCount = currentTotal
				tsLastErrors = currentErrors
			case <-ctx.Done():
				return
			}
		}
	}()

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-ticker.C:
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				result := r.doRequest(client)
				sent.Add(1)
				if result.Error != nil || result.StatusCode >= 400 {
					totalErrors.Add(1)
				}
				r.mu.Lock()
				r.Results = append(r.Results, result)
				r.mu.Unlock()
			}()
		}
	}
	wg.Wait()

	duration := time.Since(startedAt)

	r.mu.Lock()
	results := make([]RequestResult, len(r.Results))
	copy(results, r.Results)
	r.mu.Unlock()

	statusDist := map[int]int{}
	var latencies []float64
	successes := 0
	failures := 0
	for _, res := range results {
		latencies = append(latencies, float64(res.Latency.Milliseconds()))
		if res.Error != nil || res.StatusCode >= 400 {
			failures++
			if res.Error != nil {
				statusDist[0]++
			} else {
				statusDist[res.StatusCode]++
			}
		} else {
			successes++
			statusDist[res.StatusCode]++
		}
	}

	data := &report.RunData{
		RunID:     runID,
		Scenario:  "",
		StartedAt: startedAt,
		Duration:  duration,
		Config: report.RunConfig{
			TargetURL:   r.Config.TargetURL,
			RPS:         r.Config.RPS,
			Duration:    r.Config.Duration,
			Concurrency: r.Config.Concurrency,
		},
		Requests:   len(results),
		Successes:  successes,
		Failures:   failures,
		Latencies:  computeLatencyStats(latencies),
		StatusDist: statusDist,
		Timeseries: timeseries,
	}

	return data
}

func (r *Runner) doRequest(client *http.Client) RequestResult {
	start := time.Now()
	var bodyReader io.Reader
	if r.Config.Body != "" {
		bodyReader = strings.NewReader(r.Config.Body)
	}
	req, err := http.NewRequest(r.Config.Method, r.Config.TargetURL, bodyReader)
	if err != nil {
		return RequestResult{Error: err, Latency: time.Since(start), Timestamp: start}
	}
	if r.Config.Body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	latency := time.Since(start)
	if err != nil {
		return RequestResult{Error: err, Latency: latency, Timestamp: start}
	}
	resp.Body.Close()
	return RequestResult{StatusCode: resp.StatusCode, Latency: latency, Timestamp: start}
}

func (r *Runner) recentP95() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := len(r.Results)
	if n == 0 {
		return 0
	}
	start := n - 50
	if start < 0 {
		start = 0
	}
	var lats []float64
	for i := start; i < n; i++ {
		lats = append(lats, float64(r.Results[i].Latency.Milliseconds()))
	}
	sort.Float64s(lats)
	idx := int(math.Ceil(0.95*float64(len(lats)))) - 1
	if idx < 0 {
		idx = 0
	}
	return lats[idx]
}

func computeLatencyStats(latencies []float64) report.LatencyStats {
	if len(latencies) == 0 {
		return report.LatencyStats{}
	}
	sort.Float64s(latencies)
	n := len(latencies)
	sum := 0.0
	for _, l := range latencies {
		sum += l
	}
	return report.LatencyStats{
		P50: latencies[percentileIdx(n, 0.50)],
		P95: latencies[percentileIdx(n, 0.95)],
		P99: latencies[percentileIdx(n, 0.99)],
		Max: latencies[n-1],
		Avg: sum / float64(n),
	}
}

func percentileIdx(n int, pct float64) int {
	idx := int(math.Ceil(pct*float64(n))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return idx
}
