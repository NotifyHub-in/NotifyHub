package ratelimit

import (
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestManagerAllowHonorsLimitAndBurst(t *testing.T) {
	mgr := NewManager(Config{Enabled: true})

	if !mgr.Allow("client-a", rate.Limit(1), 1) {
		t.Fatal("first request should be allowed")
	}
	if mgr.Allow("client-a", rate.Limit(1), 1) {
		t.Fatal("second request should be rate limited")
	}
}

func TestManagerIsIndependentPerKey(t *testing.T) {
	mgr := NewManager(Config{Enabled: true})

	if !mgr.Allow("client-a", rate.Limit(1), 1) {
		t.Fatal("client-a first request should be allowed")
	}
	if !mgr.Allow("client-b", rate.Limit(1), 1) {
		t.Fatal("client-b first request should be allowed independently")
	}
}

func TestManagerDisabledAllowsEverything(t *testing.T) {
	mgr := NewManager(Config{Enabled: false})

	for i := 0; i < 3; i++ {
		if !mgr.Allow("client-a", rate.Limit(0.1), 1) {
			t.Fatalf("disabled manager should allow request %d", i)
		}
	}
}

func TestManagerCleansUpStaleEntries(t *testing.T) {
	mgr := NewManager(Config{
		Enabled:         true,
		CleanupInterval: 10 * time.Millisecond,
		EntryTTL:        10 * time.Millisecond,
	})

	if !mgr.Allow("client-a", rate.Limit(1), 1) {
		t.Fatal("first request should be allowed")
	}

	time.Sleep(20 * time.Millisecond)

	if !mgr.Allow("client-b", rate.Limit(1), 1) {
		t.Fatal("second key should still be allowed after cleanup")
	}

	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if len(mgr.limiters) > 1 {
		t.Fatalf("expected stale entries to be cleaned up, got %d", len(mgr.limiters))
	}
}
