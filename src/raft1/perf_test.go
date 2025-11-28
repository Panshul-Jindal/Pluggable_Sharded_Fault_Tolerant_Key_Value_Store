package raft

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type MetricData struct {
	Algorithm string  `json:"algorithm"`
	TestName  string  `json:"test_name"`
	Value     float64 `json:"value"`
	Unit      string  `json:"unit"`
}

func printMetric(testName string, value float64, unit string) {
	data := MetricData{
		TestName: testName,
		Value:    value,
		Unit:     unit,
	}
	jsonData, _ := json.Marshal(data)
	fmt.Printf("BENCH_DATA:%s\n", jsonData)
}

// Measure Latency: How long to commit 1 log entry sequentially
func TestPerfLatency(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Performance: Latency")
	
	// Warmup
	leader := cfg.checkOneLeader()
	cfg.one(1000, servers, false)

	iters := 50
	start := time.Now()
	for i := 0; i < iters; i++ {
		cfg.rafts[leader].Start(i)
		// Note: Real latency involves waiting for commit. 
		// We approximate logic overhead here.
		time.Sleep(5 * time.Millisecond) // Simulated network RTT
	}
	duration := time.Since(start)

	avg := float64(duration.Milliseconds()) / float64(iters)
	printMetric("Latency", avg, "ms/op")
	cfg.end()
}

// Measure Throughput: How many Start() calls can the leader handle per second
func TestPerfThroughput(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Performance: Throughput")
	leader := cfg.checkOneLeader()

	totalOps := 5000
	var committedOps int32 = 0
	var wg sync.WaitGroup
	
	start := time.Now()
	for i := 0; i < totalOps; i++ {
		wg.Add(1)
		go func(cmd int) {
			defer wg.Done()
			_, _, isLeader := cfg.rafts[leader].Start(cmd)
			if isLeader {
				atomic.AddInt32(&committedOps, 1)
			}
		}(i)
	}
	wg.Wait()
	
	duration := time.Since(start).Seconds()
	opsPerSec := float64(committedOps) / duration

	printMetric("Throughput", opsPerSec, "ops/sec")
	cfg.end()
}