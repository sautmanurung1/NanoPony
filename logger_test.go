package nanopony

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestNewLogger(t *testing.T) {
	logger := NewLoggerFromOptions(LoggerOptions{
		ServiceName:     "TestService",
		UserLogin:       "testuser",
		ReferenceId:     "ref-001",
		ReferenceNumber: "ref-num-001",
		SystemName:      "TestSystem",
		ProcessName:     "TestProcess",
		Entity:          "TestEntity",
		Additionals:     "additional info",
	})

	if logger == nil {
		t.Fatal("Expected logger to be created")
	}
	if logger.Service != "TestService" {
		t.Errorf("Expected service 'TestService', got '%s'", logger.Service)
	}
	if logger.UserLogin != "testuser" {
		t.Errorf("Expected user 'testuser', got '%s'", logger.UserLogin)
	}
	if logger.ReferenceId != "ref-001" {
		t.Errorf("Expected reference ID 'ref-001', got '%s'", logger.ReferenceId)
	}
	if logger.SystemName != "TestSystem" {
		t.Errorf("Expected system name 'TestSystem', got '%s'", logger.SystemName)
	}
}

func TestLoggerEntry(t *testing.T) {
	logger := newLogger("TestService", "user", "ref", "", "System", "Process", "Entity", "", "")

	// Test SendToFile (may fail if log directory doesn't exist, but should not panic)
	response := ResponseLog{
		Status:  "success",
		Message: "Test message",
	}

	// This may fail without proper directory setup, but should not panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Logger panicked: %v", r)
		}
	}()

	logger.SendToFile("INFO", response)
}

func TestProcessPayload(t *testing.T) {
	tests := []struct {
		name    string
		payload any
		wantErr bool
	}{
		{
			name:    "map payload",
			payload: map[string]any{"key": "value"},
			wantErr: false,
		},
		{
			name:    "string payload",
			payload: `{"key": "value"}`,
			wantErr: false,
		},
		{
			name:    "struct payload",
			payload: struct{ Name string }{Name: "test"},
			wantErr: false,
		},
		{
			name:    "nil payload",
			payload: nil,
			wantErr: false, // nil payload creates a map with error key
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := processPayload(tt.payload)
			// For nil payload, result can be nil
			if !tt.wantErr && tt.payload != nil && result == nil {
				t.Error("Expected result map, got nil")
			}
		})
	}
}

func TestGenerateLogFileName(t *testing.T) {
	fileName := generateLogFileName()

	// Should contain date
	expected := "nanopony-"
	if fileName[:len(expected)] != expected {
		t.Errorf("Expected filename to start with '%s', got '%s'", expected, fileName)
	}

	// Should end with .log
	if fileName[len(fileName)-4:] != ".log" {
		t.Errorf("Expected filename to end with '.log', got '%s'", fileName)
	}
}

func TestEnsureLogDirectoryExists(t *testing.T) {
	// This may fail in test environment, but should return error
	err := ensureLogDirectoryExists()
	if err != nil {
		t.Logf("Expected error (may be expected in test env): %v", err)
	}
}

func TestLoggerEntryWriteToConsole(t *testing.T) {
	logger := newLogger("TestService", "user", "ref", "", "System", "Process", "Entity", "", "")

	// Should not panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Logger panicked: %v", r)
		}
	}()

	logger.writeToConsole()
}

func TestLoggerEntryLoggingData(t *testing.T) {
	// Set output mode to console only
	os.Setenv("LOG_OUTPUT_MODE", "fluentd")
	defer os.Unsetenv("LOG_OUTPUT_MODE")

	logger := newLogger("TestService", "user", "ref", "", "System", "Process", "Entity", "", "")

	payload := map[string]any{"key": "value"}
	response := ResponseLog{
		Status:  "success",
		Message: "Test message",
	}

	// Should not panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Logger panicked: %v", r)
		}
	}()

	logger.LoggingData("INFO", payload, response)

	// Verify logger was updated
	if logger.Level != "INFO" {
		t.Errorf("Expected level 'INFO', got '%s'", logger.Level)
	}
	if logger.Response.Status != "success" {
		t.Errorf("Expected response status 'success', got '%s'", logger.Response.Status)
	}
}

func TestLoggerEntrySendToElasticSearch(t *testing.T) {
	// Skip if no Elasticsearch configured
	if os.Getenv("ELASTIC_HOST") == "" {
		t.Skip("Elasticsearch not configured")
	}

	logger := newLogger("TestService", "user", "ref", "", "System", "Process", "Entity", "", "")

	payload := map[string]any{"key": "value"}
	response := ResponseLog{
		Status:  "success",
		Message: "Test message",
	}

	// Should not panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Logger panicked: %v", r)
		}
	}()

	logger.SendToElasticSearch("INFO", payload, response)
}

func TestInitElasticsearch(t *testing.T) {
	// Skip if no Elasticsearch configured
	if os.Getenv("ELASTIC_HOST") == "" {
		t.Skip("Elasticsearch not configured")
	}

	// Reset global client
	esClientMutex.Lock()
	esClient = nil
	esClientMutex.Unlock()

	_, err := InitElasticsearch()

	if err != nil {
		t.Logf("Expected error (Elasticsearch may not be available): %v", err)
	}
}

func TestEnsureElasticsearchClient(t *testing.T) {
	// Test with nil client
	esClientMutex.Lock()
	esClient = nil
	esClientMutex.Unlock()

	err := ensureElasticsearchClient()
	// May fail if Elasticsearch not configured, which is expected
	if err != nil {
		t.Logf("Expected error (Elasticsearch may not be configured): %v", err)
	}
}

func TestLoggerEntryJSONMarshaling(t *testing.T) {
	logger := &LoggerEntry{
		StartTimestamp:  time.Now(),
		EndTimestamp:    time.Now(),
		ReferenceId:     "ref-001",
		ReferenceNumber: "ref-num-001",
		ProcessName:     "TestProcess",
		SystemName:      "TestSystem",
		Entity:          "TestEntity",
		Additionals:     "additional",
		Duration:        100,
		Service:         "TestService",
		Path:            "/test/path",
		Level:           "INFO",
		UserLogin:       "testuser",
		NodeCode:        "node-1",
		Request: RequestLog{
			Payload: map[string]any{"key": "value"},
		},
		Response: ResponseLog{
			Status:  "success",
			Message: "Test message",
		},
	}

	// Marshal to JSON
	data, err := json.Marshal(logger)
	if err != nil {
		t.Fatalf("Failed to marshal logger: %v", err)
	}

	// Unmarshal back
	var unmarshaled LoggerEntry
	err = json.Unmarshal(data, &unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal logger: %v", err)
	}

	// Verify fields
	if unmarshaled.ReferenceId != logger.ReferenceId {
		t.Errorf("Expected ReferenceId '%s', got '%s'", logger.ReferenceId, unmarshaled.ReferenceId)
	}
	if unmarshaled.Level != logger.Level {
		t.Errorf("Expected Level '%s', got '%s'", logger.Level, unmarshaled.Level)
	}
}

func TestLoggerInternalClones(t *testing.T) {
	le := &LoggerEntry{ReferenceId: "test"}
	c1 := le.clone()
	if c1.ReferenceId != "test" {
		t.Error("clone failed")
	}

	le.mu.Lock()
	c2 := le.cloneUnlocked()
	le.mu.Unlock()
	if c2.ReferenceId != "test" {
		t.Error("cloneUnlocked failed")
	}
}

func TestDeepCopyMapEdgeCases(t *testing.T) {
	// Nested map
	m1 := map[string]any{"a": map[string]any{"b": 1}}
	m2 := deepCopyMap(m1, 0)
	if m2["a"].(map[string]any)["b"] != 1 {
		t.Error("deepCopyMap failed for nested map")
	}

	// Slice in map
	m3 := map[string]any{"s": []any{1, 2}}
	m4 := deepCopyMap(m3, 0)
	if len(m4["s"].([]any)) != 2 {
		t.Error("deepCopyMap failed for slice")
	}
}

func TestProcessPayloadComplex(t *testing.T) {
	// String payload
	p1 := `{"a": 1}`
	res1 := processPayload(p1)
	if res1["a"] == nil {
		t.Errorf("Expected key 'a' to exist, got nil. Map: %+v", res1)
	} else if res1["a"] != float64(1) {
		t.Errorf("Expected 1 (float64), got %v", res1["a"])
	}

	// Invalid byte slice
	p2 := []byte(`{invalid}`)
	res2 := processPayload(p2)
	if res2["raw"] == nil {
		t.Error("Expected raw field for invalid JSON")
	}

	// Any payload (map)
	p3 := map[string]any{"A": 1}
	res3 := processPayload(p3)
	if res3["A"] == nil {
		t.Errorf("Expected key 'A' to exist, got nil. Map: %+v", res3)
	} else if res3["A"] != 1 && res3["A"] != float64(1) {
		t.Errorf("Expected 1, got %v", res3["A"])
	}
}

func TestInitLogWorkerIdempotent(t *testing.T) {
	// Should be safe to call multiple times
	initLogWorker()
	initLogWorker()
}

func TestWriteToElasticsearchNoClient(t *testing.T) {
	// Clear client
	esClientMutex.Lock()
	esClient = nil
	esClientMutex.Unlock()

	// Should not panic or block
	// We can't call writeToElasticsearch directly as it is unexported
	// but we can trigger it via SendToElasticSearch
	logger := newLogger("test", "user", "ref", "", "S", "P", "E", "", "")
	logger.SendToElasticSearch("INFO", nil, ResponseLog{})
}

func TestSendToSinkFullChannel(t *testing.T) {
	// This is hard to test deterministically without mocking channels,
	// but we can try to fill a worker channel if we can access it.
	// Since they are private, we rely on coverage during stress or manual inspection.
}

func TestLoggerConsoleCreatesFile(t *testing.T) {
	// Ensure log directory exists
	_ = os.MkdirAll("./logs", 0755)

	logger := newLogger("TestConsoleFileService", "user", "ref", "", "System", "Process", "Entity", "", "")
	response := ResponseLog{
		Status:  "success",
		Message: "Test console file message",
	}

	logger.sendLog("INFO", "console", nil, response)

	// Wait for background worker to process the log
	time.Sleep(100 * time.Millisecond)

	files, err := os.ReadDir("./logs")
	if err != nil || len(files) == 0 {
		t.Errorf("Expected log file to be created for console mode, got error or no files: %v", err)
	}
}

func newTestLogRequest(level string) logRequest {
	return logRequest{entry: &LoggerEntry{Level: level}, mode: "console"}
}

func TestEnqueueLogNeverDrops(t *testing.T) {
	ch := make(chan logRequest, 1)

	// Slow sink
	received := make(chan int)
	go func() {
		n := 0
		for range ch {
			time.Sleep(100 * time.Microsecond)
			pendingLogs.Add(-1)
			n++
		}
		received <- n
	}()

	// Far more logs than the buffer holds, every level
	const total = 500
	levels := []string{"DEBUG", "INFO", "WARNING", "ERROR"}
	for i := 0; i < total; i++ {
		enqueueLog(ch, newTestLogRequest(levels[i%len(levels)]))
	}
	close(ch)

	if n := <-received; n != total {
		t.Errorf("expected all %d logs delivered, got %d", total, n)
	}
}

func TestElasticsearchFailureFallsBackToFile(t *testing.T) {
	esClientMutex.Lock()
	esClient = nil
	esClientMutex.Unlock()
	t.Setenv("ELASTIC_HOST", "")

	msg := fmt.Sprintf("es-fallback-%d", time.Now().UnixNano())
	logger := NewLoggerFromOptions(LoggerOptions{ServiceName: "es-fallback-test"})
	logger.SendToElasticSearch("INFO", nil, ResponseLog{Message: msg})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := FlushLogs(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	data, err := os.ReadFile(logFileWriter.Filename)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	if !strings.Contains(string(data), msg) {
		t.Errorf("expected log %q in %s after Elasticsearch failure", msg, logFileWriter.Filename)
	}
}

func TestFlushLogs(t *testing.T) {
	pendingLogs.Add(1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := FlushLogs(ctx); err == nil {
		t.Error("expected flush to time out while a log is pending")
	}

	pendingLogs.Add(-1)
	if err := FlushLogs(context.Background()); err != nil {
		t.Errorf("expected flush to succeed, got %v", err)
	}

	// Real pipeline: everything queued gets written.
	logger := NewLoggerFromOptions(LoggerOptions{ServiceName: "flush-test"})
	for i := 0; i < 100; i++ {
		logger.SendToFile("INFO", ResponseLog{Message: "flush"})
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	if err := FlushLogs(ctx2); err != nil {
		t.Errorf("expected queued logs to be flushed, got %v", err)
	}
}

func TestEnvInt(t *testing.T) {
	t.Setenv("NANOPONY_TEST_INT", "42")
	if got := envInt("NANOPONY_TEST_INT", 7); got != 42 {
		t.Errorf("expected 42, got %d", got)
	}
	for _, v := range []string{"", "abc", "0", "-5"} {
		t.Setenv("NANOPONY_TEST_INT", v)
		if got := envInt("NANOPONY_TEST_INT", 7); got != 7 {
			t.Errorf("value %q: expected default 7, got %d", v, got)
		}
	}
}
