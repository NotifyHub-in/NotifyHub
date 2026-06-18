package ratelimit

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type Config struct {
	Enabled         bool
	CleanupInterval time.Duration
	EntryTTL        time.Duration
}

type Manager struct {
	enabled        bool
	cleanupEvery   time.Duration
	entryTTL       time.Duration
	mu             sync.Mutex
	limiters       map[string]*entry
	lastCleanupRun time.Time
}

type entry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
	limit    rate.Limit
	burst    int
}

func NewManager(cfg Config) *Manager {
	cleanupEvery := cfg.CleanupInterval
	if cleanupEvery <= 0 {
		cleanupEvery = 5 * time.Minute
	}
	entryTTL := cfg.EntryTTL
	if entryTTL <= 0 {
		entryTTL = 15 * time.Minute
	}
	return &Manager{
		enabled:      cfg.Enabled,
		cleanupEvery: cleanupEvery,
		entryTTL:     entryTTL,
		limiters:     make(map[string]*entry),
	}
}

func (m *Manager) Allow(key string, limit rate.Limit, burst int) bool {
	if m == nil || !m.enabled || limit <= 0 || burst <= 0 {
		return true
	}

	now := time.Now()

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.lastCleanupRun.IsZero() || now.Sub(m.lastCleanupRun) >= m.cleanupEvery {
		m.cleanupLocked(now)
		m.lastCleanupRun = now
	}

	limiterEntry, ok := m.limiters[key]
	if !ok || limiterEntry.limit != limit || limiterEntry.burst != burst {
		limiterEntry = &entry{
			limiter: rate.NewLimiter(limit, burst),
			limit:   limit,
			burst:   burst,
		}
		m.limiters[key] = limiterEntry
	}

	limiterEntry.lastSeen = now
	return limiterEntry.limiter.Allow()
}

func (m *Manager) cleanupLocked(now time.Time) {
	cutoff := now.Add(-m.entryTTL)
	for key, limiterEntry := range m.limiters {
		if limiterEntry.lastSeen.Before(cutoff) {
			delete(m.limiters, key)
		}
	}
}
