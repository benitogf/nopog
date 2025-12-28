package nopog

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"testing"
	"time"
)

// Large JSON object for benchmark
type BenchmarkData struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Timestamp   int64             `json:"timestamp"`
	Tags        []string          `json:"tags"`
	Metadata    map[string]string `json:"metadata"`
	Nested      struct {
		Field1 string   `json:"field1"`
		Field2 int      `json:"field2"`
		Field3 float64  `json:"field3"`
		Array  []string `json:"array"`
	} `json:"nested"`
	LargeText string `json:"large_text"`
}

func generateLargeJSON(i int) string {
	data := BenchmarkData{
		ID:          fmt.Sprintf("id-%d", i),
		Name:        fmt.Sprintf("benchmark-entry-%d", i),
		Description: "This is a large JSON object used for benchmarking the nopog storage system performance with realistic data sizes",
		Timestamp:   time.Now().UnixNano(),
		Tags:        []string{"benchmark", "test", "performance", "nopog", "postgresql", fmt.Sprintf("tag-%d", i%100)},
		Metadata: map[string]string{
			"created_by": "benchmark",
			"version":    "1.0.0",
			"category":   fmt.Sprintf("cat-%d", i%50),
			"priority":   fmt.Sprintf("%d", i%10),
			"status":     "active",
		},
		LargeText: "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.",
	}
	data.Nested.Field1 = fmt.Sprintf("nested-value-%d", i)
	data.Nested.Field2 = i * 100
	data.Nested.Field3 = float64(i) * 1.5
	data.Nested.Array = []string{"item1", "item2", "item3", "item4", "item5"}

	jsonBytes, _ := json.Marshal(data)
	return string(jsonBytes)
}

// Benchmark comparing single vs batch inserts
func BenchmarkWritePerformance(b *testing.B) {
	storage := &Storage{
		Name:       testServerDatabase,
		User:       testServerUser,
		Host:       testServerIP,
		Password:   testServerPassword,
		MaxRetries: 5,
	}
	err := storage.Start()
	if err != nil {
		b.Fatal(err)
	}
	defer storage.Close()

	benchTable := "benchwrite"
	err = storage.CreateTable(benchTable)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("SingleInsert_1000", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			storage.Clear(benchTable)
			for j := 0; j < 1000; j++ {
				key := fmt.Sprintf("single/%d", j)
				_, err := storage.Set(benchTable, key, generateLargeJSON(j))
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	})

	b.Run("BatchInsert_1000", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			storage.Clear(benchTable)
			keys := make([]string, 1000)
			values := make([]string, 1000)
			for j := 0; j < 1000; j++ {
				keys[j] = fmt.Sprintf("batch/%d", j)
				values[j] = generateLargeJSON(j)
			}
			_, err := storage.SetBatch(benchTable, keys, values)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	storage.DropTable(benchTable)
}

func BenchmarkLargeDatasetRangeQuery(b *testing.B) {
	storage := &Storage{
		Name:       testServerDatabase,
		User:       testServerUser,
		Host:       testServerIP,
		Password:   testServerPassword,
		MaxRetries: 5,
	}
	err := storage.Start()
	if err != nil {
		b.Fatal(err)
	}
	defer storage.Close()

	benchTable := "bench"
	err = storage.CreateTable(benchTable)
	if err != nil {
		b.Fatal(err)
	}
	storage.Clear(benchTable)

	// Insert 3 million entries
	const totalEntries = 3_000_000
	const batchSize = 10000
	const reportInterval = 100000

	log.Printf("Starting insertion of %d entries...", totalEntries)
	insertStart := time.Now()

	var firstTimestamp, middleTimestamp, lastTimestamp int64

	for i := 0; i < totalEntries; i++ {
		key := fmt.Sprintf("bench/%d", i)
		jsonData := generateLargeJSON(i)

		ts, err := storage.Set(benchTable, key, jsonData)
		if err != nil {
			b.Fatalf("failed to insert entry %d: %v", i, err)
		}

		if i == 0 {
			firstTimestamp = ts
		}
		if i == totalEntries/2 {
			middleTimestamp = ts
		}
		if i == totalEntries-1 {
			lastTimestamp = ts
		}

		if (i+1)%reportInterval == 0 {
			elapsed := time.Since(insertStart)
			rate := float64(i+1) / elapsed.Seconds()
			log.Printf("Inserted %d/%d entries (%.0f entries/sec)", i+1, totalEntries, rate)
		}
	}

	insertDuration := time.Since(insertStart)
	log.Printf("Insertion complete: %d entries in %v (%.0f entries/sec)",
		totalEntries, insertDuration, float64(totalEntries)/insertDuration.Seconds())

	// Benchmark queries
	b.ResetTimer()

	b.Run("GetNRange_1000_from_middle", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			results, err := storage.GetNRange(benchTable, "bench/*", middleTimestamp, lastTimestamp, 1000)
			if err != nil {
				b.Fatal(err)
			}
			if len(results) != 1000 {
				b.Fatalf("expected 1000 results, got %d", len(results))
			}
		}
	})

	b.Run("GetNRange_100_from_start", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			results, err := storage.GetNRange(benchTable, "bench/*", firstTimestamp, middleTimestamp, 100)
			if err != nil {
				b.Fatal(err)
			}
			if len(results) != 100 {
				b.Fatalf("expected 100 results, got %d", len(results))
			}
		}
	})

	b.Run("KeysRange_1000_from_middle", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			results, err := storage.KeysRange(benchTable, "bench/*", middleTimestamp, lastTimestamp, 1000)
			if err != nil {
				b.Fatal(err)
			}
			if len(results) != 1000 {
				b.Fatalf("expected 1000 results, got %d", len(results))
			}
		}
	})

	b.Run("GetN_100_latest", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			results, err := storage.GetN(benchTable, "bench/*", 100)
			if err != nil {
				b.Fatal(err)
			}
			if len(results) != 100 {
				b.Fatalf("expected 100 results, got %d", len(results))
			}
		}
	})

	b.Run("Get_single_key", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := "bench/" + strconv.Itoa(i%totalEntries)
			results, err := storage.Get(benchTable, key)
			if err != nil {
				b.Fatal(err)
			}
			if len(results) != 1 {
				b.Fatalf("expected 1 result, got %d", len(results))
			}
		}
	})

	// Cleanup
	b.StopTimer()
	log.Println("Cleaning up benchmark data...")
	err = storage.DropTable(benchTable)
	if err != nil {
		b.Fatal(err)
	}
	log.Println("Benchmark complete")
}

// Benchmark simulating real-world use case: years of data, query one day at a time
func BenchmarkMediumDatasetRangeQuery(b *testing.B) {
	storage := &Storage{
		Name:       testServerDatabase,
		User:       testServerUser,
		Host:       testServerIP,
		Password:   testServerPassword,
		MaxRetries: 5,
	}
	err := storage.Start()
	if err != nil {
		b.Fatal(err)
	}
	defer storage.Close()

	benchTable := "benchmed"
	err = storage.CreateTable(benchTable)
	if err != nil {
		b.Fatal(err)
	}
	storage.Clear(benchTable)

	// Insert 100k entries using batch for faster setup
	const totalEntries = 100_000
	const batchSize = 1000

	log.Printf("Starting batch insertion of %d entries...", totalEntries)
	insertStart := time.Now()

	// Track timestamps at different points to simulate time ranges
	var timestamps []int64

	for batch := 0; batch < totalEntries/batchSize; batch++ {
		keys := make([]string, batchSize)
		values := make([]string, batchSize)
		for j := 0; j < batchSize; j++ {
			idx := batch*batchSize + j
			keys[j] = fmt.Sprintf("bench/%d", idx)
			values[j] = generateLargeJSON(idx)
		}

		batchTimestamps, err := storage.SetBatch(benchTable, keys, values)
		if err != nil {
			b.Fatalf("failed to insert batch %d: %v", batch, err)
		}

		// Store first and last timestamp of each batch
		if len(batchTimestamps) > 0 {
			timestamps = append(timestamps, batchTimestamps[0])
			timestamps = append(timestamps, batchTimestamps[len(batchTimestamps)-1])
		}

		if (batch+1)%(totalEntries/batchSize/10) == 0 {
			elapsed := time.Since(insertStart)
			count := (batch + 1) * batchSize
			rate := float64(count) / elapsed.Seconds()
			log.Printf("Inserted %d/%d entries (%.0f entries/sec)", count, totalEntries, rate)
		}
	}

	insertDuration := time.Since(insertStart)
	log.Printf("Insertion complete: %d entries in %v (%.0f entries/sec)",
		totalEntries, insertDuration, float64(totalEntries)/insertDuration.Seconds())

	// Define time ranges for queries
	// Simulate "1 day" = ~1% of total data (1000 entries out of 100k)
	dayStartIdx := len(timestamps) / 100 * 2 // ~1% into the data
	dayEndIdx := dayStartIdx + 2             // One batch worth
	dayStart := timestamps[dayStartIdx]
	dayEnd := timestamps[dayEndIdx]
	middleTimestamp := timestamps[len(timestamps)/2]
	lastTimestamp := timestamps[len(timestamps)-1]

	log.Printf("Time range info: dayStart=%d, dayEnd=%d, middle=%d, last=%d",
		dayStart, dayEnd, middleTimestamp, lastTimestamp)

	b.ResetTimer()

	// Main use case: query a small time range (like one day) from large dataset
	b.Run("GetNRange_SmallWindow_100", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			results, err := storage.GetNRange(benchTable, "bench/*", dayStart, dayEnd, 100)
			if err != nil {
				b.Fatal(err)
			}
			if len(results) == 0 {
				b.Fatal("expected results")
			}
		}
	})

	b.Run("KeysRange_SmallWindow_100", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			results, err := storage.KeysRange(benchTable, "bench/*", dayStart, dayEnd, 100)
			if err != nil {
				b.Fatal(err)
			}
			if len(results) == 0 {
				b.Fatal("expected results")
			}
		}
	})

	b.Run("GetNRange_1000", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			results, err := storage.GetNRange(benchTable, "bench/*", middleTimestamp, lastTimestamp, 1000)
			if err != nil {
				b.Fatal(err)
			}
			if len(results) == 0 {
				b.Fatal("expected results")
			}
		}
	})

	b.Run("KeysRange_1000", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			results, err := storage.KeysRange(benchTable, "bench/*", middleTimestamp, lastTimestamp, 1000)
			if err != nil {
				b.Fatal(err)
			}
			if len(results) == 0 {
				b.Fatal("expected results")
			}
		}
	})

	b.Run("GetN_100", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			results, err := storage.GetN(benchTable, "bench/*", 100)
			if err != nil {
				b.Fatal(err)
			}
			if len(results) != 100 {
				b.Fatalf("expected 100 results, got %d", len(results))
			}
		}
	})

	b.Run("Get_single", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := "bench/" + strconv.Itoa(i%totalEntries)
			results, err := storage.Get(benchTable, key)
			if err != nil {
				b.Fatal(err)
			}
			if len(results) != 1 {
				b.Fatalf("expected 1 result, got %d", len(results))
			}
		}
	})

	b.StopTimer()
	storage.DropTable(benchTable)
}
