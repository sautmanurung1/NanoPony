package nanopony

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestPollerStartStop(t *testing.T) {
	pool := NewWorkerPool(2, 10, 2)
	ctx := context.Background()
	pool.Start(ctx, func(ctx context.Context, job *Job) error {
		return nil
	})
	defer pool.Stop()

	fetchCount := 0
	fetcher := DataFetcherFunc(func(ctx context.Context) ([]interface{}, error) {
		fetchCount++
		if fetchCount >= 3 {
			return []interface{}{}, nil
		}
		return []interface{}{"data1"}, nil
	})

	config := DefaultPollerConfig()
	config.Interval = 50 * time.Millisecond
	config.JobSlotSize = 1

	poller := NewPoller(config, pool, fetcher)
	poller.Start()

	if !poller.IsRunning() {
		t.Error("Expected poller to be running")
	}

	time.Sleep(200 * time.Millisecond)
	poller.Stop()

	if poller.IsRunning() {
		t.Error("Expected poller to be stopped")
	}
}

func TestDataFetcherFunc(t *testing.T) {
	expected := []interface{}{"test1", "test2"}
	fetcher := DataFetcherFunc(func(ctx context.Context) ([]interface{}, error) {
		return expected, nil
	})

	result, err := fetcher.Fetch(context.Background())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result) != len(expected) {
		t.Errorf("Expected %d items, got %d", len(expected), len(result))
	}
}

func TestPollerConfig(t *testing.T) {
	config := DefaultPollerConfig()

	if config.Interval != 1*time.Second {
		t.Errorf("Expected interval 1s, got %v", config.Interval)
	}
	if config.BatchSize != 100 {
		t.Errorf("Expected batch size 100, got %d", config.BatchSize)
	}
}

func TestPollerProcessesAllFetchedItems(t *testing.T) {
	pool := NewWorkerPool(2, 5, 1)
	ctx := context.Background()

	var processed atomic.Int64
	pool.Start(ctx, func(ctx context.Context, job *Job) error {
		time.Sleep(time.Millisecond)
		processed.Add(1)
		return nil
	})
	defer pool.Stop()

	// Two fetches return more items than BatchSize, then no more data
	var fetchCount atomic.Int64
	fetcher := DataFetcherFunc(func(ctx context.Context) ([]any, error) {
		if fetchCount.Add(1) > 2 {
			return []any{}, nil
		}
		data := make([]any, 20)
		for i := range data {
			data[i] = "item"
		}
		return data, nil
	})

	config := DefaultPollerConfig()
	config.Interval = 50 * time.Millisecond
	config.BatchSize = 5

	poller := NewPoller(config, pool, fetcher)
	poller.Start()
	defer poller.Stop()

	// Every fetched item must be processed, none cut off by BatchSize
	deadline := time.Now().Add(3 * time.Second)
	for processed.Load() < 40 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	if got := processed.Load(); got != 40 {
		t.Errorf("expected all 40 fetched items processed, got %d", got)
	}
}

func TestPollerBlockingSubmitPreventsJobLoss(t *testing.T) {
	pool := NewWorkerPool(1, 3, 1)
	ctx := context.Background()

	var processed atomic.Int64
	pool.Start(ctx, func(ctx context.Context, job *Job) error {
		time.Sleep(10 * time.Millisecond)
		processed.Add(1)
		return nil
	})
	defer pool.Stop()

	// Create fetcher that returns 10 items
	var fetchCount atomic.Int64
	fetcher := DataFetcherFunc(func(ctx context.Context) ([]any, error) {
		count := fetchCount.Add(1)
		if count >= 2 {
			return []any{}, nil
		}
		data := make([]any, 10)
		for i := range data {
			data[i] = "item"
		}
		return data, nil
	})

	config := DefaultPollerConfig()
	config.Interval = 50 * time.Millisecond
	config.JobSlotSize = 1
	config.BatchSize = 10

	poller := NewPoller(config, pool, fetcher)
	poller.Start()

	// Wait for all jobs to be processed
	time.Sleep(300 * time.Millisecond)
	poller.Stop()

	// ALL jobs should be processed (no job loss with SubmitBlocking)
	expectedJobs := int64(10) // 1 poll * 10 items
	if processed.Load() != expectedJobs {
		t.Errorf("Expected %d jobs processed (no job loss), got %d", expectedJobs, processed.Load())
	}
}

func TestPollerWaitForCompletionNoFetchOverlap(t *testing.T) {
	pool := NewWorkerPool(1, 1, 1)
	ctx := context.Background()

	var inFlight, overlaps, processed atomic.Int64
	pool.Start(ctx, func(ctx context.Context, job *Job) error {
		inFlight.Add(1)
		time.Sleep(30 * time.Millisecond) // slower than the poll interval
		processed.Add(1)
		inFlight.Add(-1)
		return nil
	})
	defer pool.Stop()

	var fetches atomic.Int64
	fetcher := DataFetcherFunc(func(ctx context.Context) ([]any, error) {
		fetches.Add(1)
		if inFlight.Load() > 0 {
			overlaps.Add(1)
		}
		return []any{"row"}, nil
	})

	config := DefaultPollerConfig()
	config.Interval = 5 * time.Millisecond
	config.WaitForCompletion = true

	poller := NewPoller(config, pool, fetcher)
	poller.Start()
	time.Sleep(200 * time.Millisecond)
	poller.Stop()

	if overlaps.Load() != 0 {
		t.Errorf("Fetch overlapped with job processing %d times", overlaps.Load())
	}
	if fetches.Load() < 2 {
		t.Errorf("expected poller to keep fetching after jobs finish, got %d fetches", fetches.Load())
	}
	if processed.Load() == 0 {
		t.Error("expected jobs to be processed")
	}
}

func TestJobReleaseCallsOnDoneOnce(t *testing.T) {
	var calls atomic.Int64
	job := AcquireJob()
	job.onDone = func() { calls.Add(1) }

	job.Release()
	job.Release()

	if calls.Load() != 1 {
		t.Errorf("expected onDone to run once, got %d", calls.Load())
	}
}
