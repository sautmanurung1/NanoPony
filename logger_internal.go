package nanopony

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/natefinch/lumberjack"
)

// Global logging state
var (
	// esClient is protected by esClientMutex for thread safety.
	esClient      *elasticsearch.Client
	esClientMutex sync.RWMutex

	logFileWriter *lumberjack.Logger
	onceLogger    sync.Once

	logChan     chan logRequest
	consoleChan chan logRequest
	fileChan    chan logRequest
	esChan      chan logRequest
	onceWorker  sync.Once

	cachedWd     string
	onceCachedWd sync.Once

	onceDirChecked sync.Once

	// Overflow handling, configured once from env in initLogWorker.
	logOverflowBlock bool          // LOG_OVERFLOW_POLICY=block
	logBlockTimeout  time.Duration // LOG_BLOCK_TIMEOUT_MS
	droppedLogs      atomic.Int64  // total logs dropped since start
	pendingLogs      atomic.Int64  // logs queued but not yet written, per sink
)

const (
	defaultLogBufferSize   = 10000
	defaultLogBlockTimeout = 50 * time.Millisecond
	logDropReportInterval  = 10 * time.Second
)

// DroppedLogs returns how many log writes were dropped because a log buffer was full.
// In hybrid mode each sink (console, file, elasticsearch) counts separately.
func DroppedLogs() int64 {
	return droppedLogs.Load()
}

// FlushLogs waits until every queued log has been written, or ctx is done.
func FlushLogs(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for pendingLogs.Load() > 0 {
		select {
		case <-ctx.Done():
			return fmt.Errorf("flush logs: %d still pending: %w", pendingLogs.Load(), ctx.Err())
		case <-ticker.C:
		}
	}
	return nil
}

// envInt reads a positive integer env var, falling back to def.
func envInt(name string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(name)); err == nil && n > 0 {
		return n
	}
	return def
}

// enqueueLog puts req on ch, applying the overflow policy when ch is full.
// ERROR and FATAL logs always wait for space so they are never dropped.
// Returns false when the log was dropped.
func enqueueLog(ch chan logRequest, req logRequest) bool {
	select {
	case ch <- req:
		return true
	default:
	}

	switch strings.ToUpper(req.entry.Level) {
	case "ERROR", "FATAL":
		ch <- req
		return true
	}

	if logOverflowBlock {
		timer := time.NewTimer(logBlockTimeout)
		defer timer.Stop()
		select {
		case ch <- req:
			return true
		case <-timer.C:
		}
	}

	droppedLogs.Add(1)
	return false
}

// reportDroppedLogs prints one summary line per interval instead of one line per dropped log.
func reportDroppedLogs() {
	var last int64
	for range time.Tick(logDropReportInterval) {
		if total := droppedLogs.Load(); total > last {
			fmt.Printf("[WARNING] dropped %d logs in last %s (total %d)\n", total-last, logDropReportInterval, total)
			last = total
		}
	}
}

// logRequest represents an internal request to process a log entry.
type logRequest struct {
	entry *LoggerEntry
	mode  string
}

// clone creates a safe copy of the LoggerEntry metadata without copying the mutex.
func (le *LoggerEntry) clone() *LoggerEntry {
	le.mu.RLock()
	defer le.mu.RUnlock()

	return le.cloneUnlocked()
}

// cloneUnlocked creates a copy of the LoggerEntry without acquiring the lock.
// The caller MUST hold le.mu (either RLock or Lock) before calling.
func (le *LoggerEntry) cloneUnlocked() *LoggerEntry {
	return &LoggerEntry{
		StartTimestamp:  le.StartTimestamp,
		EndTimestamp:    le.EndTimestamp,
		ReferenceId:     le.ReferenceId,
		ReferenceNumber: le.ReferenceNumber,
		ProcessName:     le.ProcessName,
		SystemName:      le.SystemName,
		Entity:          le.Entity,
		Additionals:     le.Additionals,
		Duration:        le.Duration,
		Service:         le.Service,
		Path:            le.Path,
		Level:           le.Level,
		UserLogin:       le.UserLogin,
		NodeCode:        le.NodeCode,
		Request:         le.Request,
		Response:        le.Response,
	}
}

// initLogWorker initializes the background log processing goroutines exactly once.
func initLogWorker() {
	onceWorker.Do(func() {
		size := envInt("LOG_BUFFER_SIZE", defaultLogBufferSize)
		logOverflowBlock = strings.EqualFold(os.Getenv("LOG_OVERFLOW_POLICY"), "block")
		logBlockTimeout = time.Duration(envInt("LOG_BLOCK_TIMEOUT_MS", int(defaultLogBlockTimeout/time.Millisecond))) * time.Millisecond

		logChan = make(chan logRequest, size)
		consoleChan = make(chan logRequest, size)
		fileChan = make(chan logRequest, size)
		esChan = make(chan logRequest, size)

		go reportDroppedLogs()

		// Main Dispatcher Worker
		go func() {
			for req := range logChan {
				dispatchLogRequest(req)
				pendingLogs.Add(-1)
			}
		}()

		// Dedicated Console Worker (Realtime)
		go func() {
			for req := range consoleChan {
				req.entry.writeToConsole()
				if req.mode == "console" {
					req.entry.writeToLogFile()
				}
				pendingLogs.Add(-1)
			}
		}()

		// Dedicated File Worker (Can be slow)
		go func() {
			for req := range fileChan {
				req.entry.writeToLogFile()
				pendingLogs.Add(-1)
			}
		}()

		// Dedicated Elasticsearch Worker (Realtime/Network dependent)
		go func() {
			for req := range esChan {
				// Use the payload already inside req.entry
				req.entry.writeToElasticsearch(nil)
				pendingLogs.Add(-1)
			}
		}()
	})
}

// dispatchLogRequest routes a log request to the appropriate sink workers.
func dispatchLogRequest(req logRequest) {
	switch req.mode {
	case "console":
		sendToSink(consoleChan, req)
	case "file":
		sendToSink(fileChan, req)
	case "elasticsearch":
		sendToSink(esChan, req)
	case "hybrid":
		// Create clones for parallel processing.
		// Each clone gets its own LoggerEntry metadata but shares the same
		// processed (read-only) payload map.
		ce1 := req.entry.clone()
		ce2 := req.entry.clone()

		sendToSink(consoleChan, logRequest{entry: ce1, mode: req.mode})
		sendToSink(fileChan, logRequest{entry: ce2, mode: req.mode})
		sendToSink(esChan, logRequest{entry: req.entry, mode: req.mode})
	}
}

// sendToSink sends a request to a sink channel using the overflow policy.
// If a specific sink stays full, the log is dropped for THAT sink only.
func sendToSink(ch chan logRequest, req logRequest) {
	if ch == nil {
		return
	}
	pendingLogs.Add(1)
	if !enqueueLog(ch, req) {
		pendingLogs.Add(-1)
	}
}

// initLoggerFile sets up the rolling file writer exactly once.
func initLoggerFile() {
	onceLogger.Do(func() {
		onceDirChecked.Do(func() {
			_ = ensureLogDirectoryExists()
		})

		logFileName := generateLogFileName()
		logFilePath := path.Join(LogDir, logFileName)

		logFileWriter = &lumberjack.Logger{
			Filename:   logFilePath,
			MaxSize:    100,
			MaxBackups: 3,
			MaxAge:     28,
			Compress:   false,
		}
	})
}

// writeToLogFile serializes the entry to JSON and appends it to the rolling log file.
func (le *LoggerEntry) writeToLogFile() {
	le.mu.RLock()
	logMessage, err := json.Marshal(le)
	le.mu.RUnlock()

	if err != nil {
		fmt.Fprintf(os.Stderr, "[NanoPony-Logger] Failed to marshal log for file: %v\n", err)
		return
	}

	if logFileWriter != nil {
		if _, err := fmt.Fprintf(logFileWriter, "%s\n", string(logMessage)); err != nil {
			fmt.Fprintf(os.Stderr, "[NanoPony-Logger] Failed to write to log file: %v\n", err)
		}
	}
}

// writeToConsole prints the entry as JSON to stdout.
func (le *LoggerEntry) writeToConsole() {
	le.mu.RLock()
	data, err := json.Marshal(le)
	le.mu.RUnlock()

	if err != nil {
		fmt.Fprintf(os.Stderr, "[NanoPony-Logger] Failed to marshal log for console: %v\n", err)
		return
	}

	fmt.Println(string(data))
}

// writeToElasticsearch sends the entry and its payload to an Elasticsearch index.
// The index name follows the pattern: <ELASTIC_PREFIX_INDEX><YYYYMMDD>
func (le *LoggerEntry) writeToElasticsearch(payload any) {
	if err := ensureElasticsearchClient(); err != nil {
		return
	}

	conf := getAppConfig()
	if conf == nil {
		return
	}

	payloadMap := processPayload(payload)

	le.mu.Lock()
	le.Request.Payload = payloadMap
	data, err := json.Marshal(le)
	le.mu.Unlock()

	if err != nil {
		fmt.Fprintf(os.Stderr, "[NanoPony-Logger] Failed to marshal log for Elasticsearch: %v\n", err)
		return
	}

	esIndexWithDate := conf.ElasticSearch.ElasticPrefixIndex + time.Now().Format("20060102")
	res, err := esClient.Index(esIndexWithDate, bytes.NewReader(data))
	if err != nil {
		fmt.Fprintf(os.Stderr, "[NanoPony-Logger] Failed to send log to Elasticsearch: %v\n", err)
		return
	}
	_ = res.Body.Close()
}

// processPayload normalizes various payload types into a map[string]any for JSON logging.
func processPayload(payload any) map[string]any {
	if payload == nil {
		return nil
	}

	switch p := payload.(type) {
	case map[string]any:
		return deepCopyMap(p, 0)
	case string:
		var m map[string]any
		if err := json.Unmarshal([]byte(p), &m); err != nil {
			return map[string]any{"raw": p}
		}
		return m
	case []byte:
		var m map[string]any
		if err := json.Unmarshal(p, &m); err != nil {
			return map[string]any{"raw": string(p)}
		}
		return m
	default:
		// Fallback to JSON marshalling for other types (structs, etc.)
		dataBytes, err := json.Marshal(payload)
		if err != nil {
			return map[string]any{"error": "failed_to_marshal", "raw": fmt.Sprintf("%v", payload)}
		}

		var payloadMap map[string]any
		if err := json.Unmarshal(dataBytes, &payloadMap); err != nil {
			return map[string]any{"raw": string(dataBytes)}
		}
		return payloadMap
	}
}

const maxCopyDepth = 10

// deepCopyMap performs a shallow copy of a map, and recursively copies nested maps.
// This is faster than json.Marshal/Unmarshal for map deep copying.
// depth parameter prevents stack overflow from circular references.
func deepCopyMap(m map[string]any, depth int) map[string]any {
	if depth > maxCopyDepth {
		return map[string]any{"error": "max_copy_depth_reached"}
	}

	cp := make(map[string]any, len(m))
	for k, v := range m {
		if vm, ok := v.(map[string]any); ok {
			cp[k] = deepCopyMap(vm, depth+1)
		} else {
			cp[k] = v
		}
	}
	return cp
}

// generateLogFileName creates a daily log file name with an optional prefix from config.
func generateLogFileName() string {
	now := time.Now()
	prefix := "nanopony"
	if conf := getAppConfig(); conf != nil && conf.App.LogFilePrefix != "" {
		prefix = conf.App.LogFilePrefix
	}
	return fmt.Sprintf("%s-%d-%02d-%02d.log", prefix, now.Year(), int(now.Month()), now.Day())
}

// ensureLogDirectoryExists creates the log directory if it doesn't already exist.
func ensureLogDirectoryExists() error {
	return os.MkdirAll(LogDir, 0755)
}

// ensureElasticsearchClient initializes the ES client if it hasn't been initialized yet.
func ensureElasticsearchClient() error {
	esClientMutex.Lock()
	defer esClientMutex.Unlock()
	if esClient != nil {
		return nil
	}
	client, err := InitElasticsearch()
	if err != nil {
		return err
	}
	esClient = client
	return nil
}
