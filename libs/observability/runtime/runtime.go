package runtime

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	obsmetrics "github.com/Arunshaik2001/notification-control-plane/libs/observability/metrics"
)

func Register(registry *obsmetrics.Registry, startedAt time.Time) {
	if registry == nil {
		return
	}

	labels := map[string]string{"service": registry.Service()}

	registry.SetGaugeFunc("go_goroutines", "Number of goroutines currently running.", labels, func() float64 {
		return float64(runtime.NumGoroutine())
	})
	registry.SetGaugeFunc("process_uptime_seconds", "Process uptime in seconds.", labels, func() float64 {
		return time.Since(startedAt).Seconds()
	})
	registry.SetGaugeFunc("go_memstats_alloc_bytes", "Number of bytes allocated and still in use.", labels, func() float64 {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		return float64(mem.Alloc)
	})
	registry.SetGaugeFunc("go_memstats_heap_inuse_bytes", "Number of heap bytes marked as in-use.", labels, func() float64 {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		return float64(mem.HeapInuse)
	})
	registry.SetGaugeFunc("go_memstats_stack_inuse_bytes", "Number of stack bytes marked as in-use.", labels, func() float64 {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		return float64(mem.StackInuse)
	})
	registry.SetGaugeFunc("go_gc_last_duration_seconds", "Duration of the most recent garbage collection pause in seconds.", labels, func() float64 {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		if mem.NumGC == 0 {
			return 0
		}
		index := (mem.NumGC - 1) % uint32(len(mem.PauseNs))
		return float64(mem.PauseNs[index]) / float64(time.Second)
	})

	sampler := newProcessSampler()
	registry.SetGaugeFunc("process_cpu_usage_percent", "Process CPU usage percent over the most recent scrape interval.", labels, sampler.cpuPercent)
	registry.SetGaugeFunc("process_resident_memory_bytes", "Resident memory size in bytes for the current process.", labels, sampler.residentMemoryBytes)
	registry.SetGaugeFunc("process_virtual_memory_bytes", "Virtual memory size in bytes for the current process.", labels, sampler.virtualMemoryBytes)
	registry.SetGaugeFunc("process_threads", "Operating system thread count for the current process.", labels, sampler.threadCount)
}

type processSampler struct {
	mu          sync.Mutex
	lastWall    time.Time
	lastCPU     float64
	initialized bool
}

func newProcessSampler() *processSampler {
	return &processSampler{}
}

func (s *processSampler) cpuPercent() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	cpuSeconds, ok := readProcessCPUSeconds()
	if !ok {
		return 0
	}

	now := time.Now()
	if !s.initialized {
		s.lastWall = now
		s.lastCPU = cpuSeconds
		s.initialized = true
		return 0
	}

	elapsedWall := now.Sub(s.lastWall).Seconds()
	elapsedCPU := cpuSeconds - s.lastCPU
	s.lastWall = now
	s.lastCPU = cpuSeconds

	if elapsedWall <= 0 || elapsedCPU < 0 {
		return 0
	}

	return (elapsedCPU / elapsedWall) * 100
}

func (s *processSampler) residentMemoryBytes() float64 {
	rssBytes, _, _, ok := readProcessStatus()
	if !ok {
		return 0
	}
	return float64(rssBytes)
}

func (s *processSampler) virtualMemoryBytes() float64 {
	_, virtualBytes, _, ok := readProcessStatus()
	if !ok {
		return 0
	}
	return float64(virtualBytes)
}

func (s *processSampler) threadCount() float64 {
	_, _, threads, ok := readProcessStatus()
	if !ok {
		return 0
	}
	return float64(threads)
}

func readProcessCPUSeconds() (float64, bool) {
	if runtime.GOOS != "linux" {
		return 0, false
	}

	payload, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return 0, false
	}

	content := string(payload)
	closing := strings.LastIndex(content, ")")
	if closing == -1 || closing+2 >= len(content) {
		return 0, false
	}

	fields := strings.Fields(content[closing+2:])
	if len(fields) <= 12 {
		return 0, false
	}

	utimeTicks, err := strconv.ParseFloat(fields[11], 64)
	if err != nil {
		return 0, false
	}
	stimeTicks, err := strconv.ParseFloat(fields[12], 64)
	if err != nil {
		return 0, false
	}

	const ticksPerSecond = 100.0
	return (utimeTicks + stimeTicks) / ticksPerSecond, true
}

func readProcessStatus() (residentBytes uint64, virtualBytes uint64, threads int, ok bool) {
	if runtime.GOOS != "linux" {
		return 0, 0, 0, false
	}

	payload, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, 0, 0, false
	}

	var foundRSS bool
	var foundVirtual bool
	var foundThreads bool

	for _, line := range strings.Split(string(payload), "\n") {
		switch {
		case strings.HasPrefix(line, "VmRSS:"):
			value, parsed := parseStatusKB(line)
			if parsed {
				residentBytes = value * 1024
				foundRSS = true
			}
		case strings.HasPrefix(line, "VmSize:"):
			value, parsed := parseStatusKB(line)
			if parsed {
				virtualBytes = value * 1024
				foundVirtual = true
			}
		case strings.HasPrefix(line, "Threads:"):
			value, parsed := parseStatusInt(line)
			if parsed {
				threads = value
				foundThreads = true
			}
		}
	}

	return residentBytes, virtualBytes, threads, foundRSS || foundVirtual || foundThreads
}

func parseStatusKB(line string) (uint64, bool) {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return 0, false
	}
	value, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return value, true
}

func parseStatusInt(line string) (int, bool) {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return 0, false
	}
	value, err := strconv.Atoi(fields[1])
	if err != nil {
		return 0, false
	}
	return value, true
}
