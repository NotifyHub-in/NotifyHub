package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Registry struct {
	service    string
	mu         sync.Mutex
	counters   map[string]*counterMetric
	gauges     map[string]*gaugeMetric
	summaries  map[string]*summaryMetric
	histograms map[string]*histogramMetric
}

type counterMetric struct {
	help   string
	values map[string]metricValue
}

type summaryMetric struct {
	help   string
	values map[string]summaryValue
}

type gaugeMetric struct {
	help      string
	values    map[string]metricValue
	callbacks map[string]gaugeCallback
}

type histogramMetric struct {
	help    string
	buckets []float64
	values  map[string]histogramValue
}

type metricValue struct {
	labels map[string]string
	value  float64
}

type summaryValue struct {
	labels map[string]string
	sum    float64
	count  uint64
}

type gaugeCallback struct {
	labels map[string]string
	fn     func() float64
}

type histogramValue struct {
	labels  map[string]string
	sum     float64
	count   uint64
	buckets []uint64
}

func NewRegistry(service string) *Registry {
	return &Registry{
		service:    service,
		counters:   make(map[string]*counterMetric),
		gauges:     make(map[string]*gaugeMetric),
		summaries:  make(map[string]*summaryMetric),
		histograms: make(map[string]*histogramMetric),
	}
}

func (r *Registry) Service() string {
	return r.service
}

func (r *Registry) IncCounter(name string, help string, labels map[string]string) {
	r.AddCounter(name, help, labels, 1)
}

func (r *Registry) AddCounter(name string, help string, labels map[string]string, delta float64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	metric := r.counters[name]
	if metric == nil {
		metric = &counterMetric{
			help:   help,
			values: make(map[string]metricValue),
		}
		r.counters[name] = metric
	}

	key, copiedLabels := labelsKey(labels)
	entry := metric.values[key]
	if entry.labels == nil {
		entry.labels = copiedLabels
	}
	entry.value += delta
	metric.values[key] = entry
}

func (r *Registry) SetGauge(name string, help string, labels map[string]string, value float64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	metric := r.gauges[name]
	if metric == nil {
		metric = &gaugeMetric{
			help:      help,
			values:    make(map[string]metricValue),
			callbacks: make(map[string]gaugeCallback),
		}
		r.gauges[name] = metric
	}

	key, copiedLabels := labelsKey(labels)
	metric.values[key] = metricValue{
		labels: copiedLabels,
		value:  value,
	}
	delete(metric.callbacks, key)
}

func (r *Registry) SetGaugeFunc(name string, help string, labels map[string]string, fn func() float64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	metric := r.gauges[name]
	if metric == nil {
		metric = &gaugeMetric{
			help:      help,
			values:    make(map[string]metricValue),
			callbacks: make(map[string]gaugeCallback),
		}
		r.gauges[name] = metric
	}

	key, copiedLabels := labelsKey(labels)
	metric.callbacks[key] = gaugeCallback{
		labels: copiedLabels,
		fn:     fn,
	}
	delete(metric.values, key)
}

func (r *Registry) ObserveSummary(name string, help string, labels map[string]string, value float64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	metric := r.summaries[name]
	if metric == nil {
		metric = &summaryMetric{
			help:   help,
			values: make(map[string]summaryValue),
		}
		r.summaries[name] = metric
	}

	key, copiedLabels := labelsKey(labels)
	entry := metric.values[key]
	if entry.labels == nil {
		entry.labels = copiedLabels
	}
	entry.sum += value
	entry.count++
	metric.values[key] = entry
}

func (r *Registry) ObserveHistogram(name string, help string, labels map[string]string, buckets []float64, value float64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	metric := r.histograms[name]
	if metric == nil {
		metric = &histogramMetric{
			help:    help,
			buckets: append([]float64(nil), buckets...),
			values:  make(map[string]histogramValue),
		}
		r.histograms[name] = metric
	}

	key, copiedLabels := labelsKey(labels)
	entry := metric.values[key]
	if entry.labels == nil {
		entry.labels = copiedLabels
		entry.buckets = make([]uint64, len(metric.buckets))
	}
	entry.sum += value
	entry.count++
	for i, upperBound := range metric.buckets {
		if value <= upperBound {
			entry.buckets[i]++
		}
	}
	metric.values[key] = entry
}

func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(r.Render()))
	})
}

func (r *Registry) Render() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	var builder strings.Builder

	counterNames := make([]string, 0, len(r.counters))
	for name := range r.counters {
		counterNames = append(counterNames, name)
	}
	sort.Strings(counterNames)

	for _, name := range counterNames {
		metric := r.counters[name]
		builder.WriteString("# HELP ")
		builder.WriteString(name)
		builder.WriteString(" ")
		builder.WriteString(metric.help)
		builder.WriteString("\n# TYPE ")
		builder.WriteString(name)
		builder.WriteString(" counter\n")

		keys := sortedMetricKeys(metric.values)
		for _, key := range keys {
			entry := metric.values[key]
			builder.WriteString(name)
			builder.WriteString(formatLabels(entry.labels))
			builder.WriteString(" ")
			builder.WriteString(strconv.FormatFloat(entry.value, 'f', -1, 64))
			builder.WriteString("\n")
		}
	}

	gaugeNames := make([]string, 0, len(r.gauges))
	for name := range r.gauges {
		gaugeNames = append(gaugeNames, name)
	}
	sort.Strings(gaugeNames)

	for _, name := range gaugeNames {
		metric := r.gauges[name]
		builder.WriteString("# HELP ")
		builder.WriteString(name)
		builder.WriteString(" ")
		builder.WriteString(metric.help)
		builder.WriteString("\n# TYPE ")
		builder.WriteString(name)
		builder.WriteString(" gauge\n")

		valueKeys := sortedMetricKeys(metric.values)
		for _, key := range valueKeys {
			entry := metric.values[key]
			builder.WriteString(name)
			builder.WriteString(formatLabels(entry.labels))
			builder.WriteString(" ")
			builder.WriteString(strconv.FormatFloat(entry.value, 'f', -1, 64))
			builder.WriteString("\n")
		}

		callbackKeys := sortedMetricKeys(metric.callbacks)
		for _, key := range callbackKeys {
			entry := metric.callbacks[key]
			builder.WriteString(name)
			builder.WriteString(formatLabels(entry.labels))
			builder.WriteString(" ")
			builder.WriteString(strconv.FormatFloat(entry.fn(), 'f', -1, 64))
			builder.WriteString("\n")
		}
	}

	summaryNames := make([]string, 0, len(r.summaries))
	for name := range r.summaries {
		summaryNames = append(summaryNames, name)
	}
	sort.Strings(summaryNames)

	for _, name := range summaryNames {
		metric := r.summaries[name]
		builder.WriteString("# HELP ")
		builder.WriteString(name)
		builder.WriteString(" ")
		builder.WriteString(metric.help)
		builder.WriteString("\n# TYPE ")
		builder.WriteString(name)
		builder.WriteString(" summary\n")

		keys := sortedMetricKeys(metric.values)
		for _, key := range keys {
			entry := metric.values[key]
			builder.WriteString(name)
			builder.WriteString("_sum")
			builder.WriteString(formatLabels(entry.labels))
			builder.WriteString(" ")
			builder.WriteString(strconv.FormatFloat(entry.sum, 'f', -1, 64))
			builder.WriteString("\n")

			builder.WriteString(name)
			builder.WriteString("_count")
			builder.WriteString(formatLabels(entry.labels))
			builder.WriteString(" ")
			builder.WriteString(strconv.FormatUint(entry.count, 10))
			builder.WriteString("\n")
		}
	}

	histogramNames := make([]string, 0, len(r.histograms))
	for name := range r.histograms {
		histogramNames = append(histogramNames, name)
	}
	sort.Strings(histogramNames)

	for _, name := range histogramNames {
		metric := r.histograms[name]
		builder.WriteString("# HELP ")
		builder.WriteString(name)
		builder.WriteString(" ")
		builder.WriteString(metric.help)
		builder.WriteString("\n# TYPE ")
		builder.WriteString(name)
		builder.WriteString(" histogram\n")

		keys := sortedMetricKeys(metric.values)
		for _, key := range keys {
			entry := metric.values[key]
			for i, upperBound := range metric.buckets {
				bucketLabels := copyLabels(entry.labels)
				bucketLabels["le"] = strconv.FormatFloat(upperBound, 'f', -1, 64)
				builder.WriteString(name)
				builder.WriteString("_bucket")
				builder.WriteString(formatLabels(bucketLabels))
				builder.WriteString(" ")
				builder.WriteString(strconv.FormatUint(entry.buckets[i], 10))
				builder.WriteString("\n")
			}

			infLabels := copyLabels(entry.labels)
			infLabels["le"] = "+Inf"
			builder.WriteString(name)
			builder.WriteString("_bucket")
			builder.WriteString(formatLabels(infLabels))
			builder.WriteString(" ")
			builder.WriteString(strconv.FormatUint(entry.count, 10))
			builder.WriteString("\n")

			builder.WriteString(name)
			builder.WriteString("_sum")
			builder.WriteString(formatLabels(entry.labels))
			builder.WriteString(" ")
			builder.WriteString(strconv.FormatFloat(entry.sum, 'f', -1, 64))
			builder.WriteString("\n")

			builder.WriteString(name)
			builder.WriteString("_count")
			builder.WriteString(formatLabels(entry.labels))
			builder.WriteString(" ")
			builder.WriteString(strconv.FormatUint(entry.count, 10))
			builder.WriteString("\n")
		}
	}

	return builder.String()
}

func DefaultLatencyBuckets() []float64 {
	return []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}
}

func sortedMetricKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func labelsKey(labels map[string]string) (string, map[string]string) {
	if len(labels) == 0 {
		return "", nil
	}

	keys := make([]string, 0, len(labels))
	copied := make(map[string]string, len(labels))
	for key, value := range labels {
		keys = append(keys, key)
		copied[key] = value
	}
	sort.Strings(keys)

	var builder strings.Builder
	for _, key := range keys {
		builder.WriteString(key)
		builder.WriteString("=")
		builder.WriteString(labels[key])
		builder.WriteString(";")
	}
	return builder.String(), copied
}

func copyLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}

	copied := make(map[string]string, len(labels))
	for key, value := range labels {
		copied[key] = value
	}
	return copied
}

func formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}

	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var parts []string
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf(`%s=%q`, key, labels[key]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}
