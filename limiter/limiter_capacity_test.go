package limiter

import (
	"fmt"
	"testing"
	"time"
)

// TestTokenBucketCapacityBound verifies that the number of cached per-key token
// buckets never exceeds the configured upper bound, no matter how many unique
// keys (e.g. forged forwarding headers or unique paths) are fed in.
//
// Note the difference between the two ways an entry leaves the cache:
//   - capacity eviction happens synchronously inside Set/LimitReached, the
//     moment an insertion would make the cache exceed MaxTokenBuckets, and it
//     applies to entries that have not expired yet;
//   - TTL expiry is time based and, in this cache, only reclaims memory when
//     DeleteExpiredTokenBuckets (or a later insertion that reuses the slot) runs,
//     because the cache does not spawn a background sweeper.
func TestTokenBucketCapacityBound(t *testing.T) {
	const maxBuckets = 100
	const uniqueKeys = 5000

	lmt := New(&ExpirableOptions{MaxTokenBuckets: maxBuckets}).SetMax(1).SetBurst(1)

	if got := lmt.GetMaxTokenBuckets(); got != maxBuckets {
		t.Fatalf("GetMaxTokenBuckets() = %d, want %d", got, maxBuckets)
	}

	for i := 0; i < uniqueKeys; i++ {
		lmt.LimitReached(fmt.Sprintf("10.0.%d.%d|/path-%d", i/256, i%256, i))
	}

	if got := lmt.tokenBuckets.Len(); got > maxBuckets {
		t.Fatalf("token bucket cache holds %d entries, want at most %d", got, maxBuckets)
	}
}

// TestTokenBucketCapacityDefault verifies the default and the opt-out semantics
// of MaxTokenBuckets: zero selects DefaultMaxTokenBuckets, negative means
// unlimited (the pre-existing behavior).
func TestTokenBucketCapacityDefault(t *testing.T) {
	if got := New(nil).GetMaxTokenBuckets(); got != DefaultMaxTokenBuckets {
		t.Errorf("default GetMaxTokenBuckets() = %d, want %d", got, DefaultMaxTokenBuckets)
	}

	if got := New(&ExpirableOptions{MaxTokenBuckets: -1}).GetMaxTokenBuckets(); got != 0 {
		t.Errorf("opt-out GetMaxTokenBuckets() = %d, want 0 (unlimited)", got)
	}
}

// TestTokenBucketActiveKeySurvivesEviction verifies that a key which is still
// receiving traffic is not evicted when a flood of unique keys exceeds the
// capacity. The hot key's bucket must keep its exhausted state, so the limiting
// decision does not regress (an evicted-then-recreated bucket would come back
// with a full burst and let the request through).
func TestTokenBucketActiveKeySurvivesEviction(t *testing.T) {
	const maxBuckets = 10
	const hotKey = "192.0.2.1|/hot"

	// 0.1 req/s with burst 1: one request allowed, then rejected for ~10s,
	// which is far longer than this test runs.
	lmt := New(&ExpirableOptions{MaxTokenBuckets: maxBuckets}).SetMax(0.1).SetBurst(1)

	if lmt.LimitReached(hotKey) {
		t.Fatalf("first request for hot key must be allowed")
	}
	if !lmt.LimitReached(hotKey) {
		t.Fatalf("second request for hot key must be rejected")
	}

	// Fill the cache up to capacity with cold keys.
	for i := 0; i < maxBuckets-1; i++ {
		lmt.LimitReached(fmt.Sprintf("198.51.100.%d|/cold-%d", i+1, i))
	}
	if got := lmt.tokenBuckets.Len(); got != maxBuckets {
		t.Fatalf("cache holds %d entries, want exactly %d", got, maxBuckets)
	}

	// Keep the hot key active, interleaved with the flood: LRU must evict cold
	// keys, never the recently used one.
	for i := 0; i < maxBuckets; i++ {
		if !lmt.LimitReached(hotKey) {
			t.Fatalf("hot key lost its exhausted bucket at flood iteration %d", i)
		}
		lmt.LimitReached(fmt.Sprintf("203.0.113.%d|/flood-%d", i+1, i))
	}

	if !lmt.tokenBuckets.Contains(hotKey) {
		t.Fatalf("hot key was evicted while still receiving traffic")
	}
	if !lmt.LimitReached(hotKey) {
		t.Fatalf("hot key limiting decision regressed after the flood")
	}
	if got := lmt.tokenBuckets.Len(); got > maxBuckets {
		t.Fatalf("token bucket cache holds %d entries, want at most %d", got, maxBuckets)
	}
}

// TestDeleteExpiredTokenBuckets verifies the cleanup API signature and behavior:
// it removes only TTL-expired entries and never touches live ones. Capacity
// eviction, by contrast, is driven by insertion count and does not need (nor
// wait for) this call.
func TestDeleteExpiredTokenBuckets(t *testing.T) {
	const numKeys = 50

	lmt := New(&ExpirableOptions{
		DefaultExpirationTTL: 300 * time.Millisecond,
		MaxTokenBuckets:      numKeys * 10,
	}).SetMax(1).SetBurst(1)

	for i := 0; i < numKeys; i++ {
		lmt.LimitReached(fmt.Sprintf("192.0.2.%d|/key-%d", i+1, i))
	}

	if got := lmt.tokenBuckets.Len(); got != numKeys {
		t.Fatalf("cache holds %d entries before expiry, want %d", got, numKeys)
	}

	// Expired entries stay resident (Len counts them) until they are swept.
	time.Sleep(400 * time.Millisecond)
	if got := lmt.tokenBuckets.Len(); got != numKeys {
		t.Fatalf("cache holds %d entries after TTL elapsed, want %d: expiry must not be eager", got, numKeys)
	}

	lmt.DeleteExpiredTokenBuckets()

	if got := lmt.tokenBuckets.Len(); got != 0 {
		t.Fatalf("cache holds %d entries after cleanup, want 0", got)
	}
}

// TestDeleteExpiredTokenBucketsKeepsLiveKeys guards the cleanup API from
// over-deleting: live buckets created inside the newest TTL window survive a
// sweep while expired ones are removed.
func TestDeleteExpiredTokenBucketsKeepsLiveKeys(t *testing.T) {
	lmt := New(&ExpirableOptions{
		DefaultExpirationTTL: 300 * time.Millisecond,
		MaxTokenBuckets:      100,
	}).SetMax(1).SetBurst(1)

	lmt.LimitReached("192.0.2.1|/expired")
	time.Sleep(350 * time.Millisecond)
	lmt.LimitReached("192.0.2.2|/live")

	lmt.DeleteExpiredTokenBuckets()

	if lmt.tokenBuckets.Contains("192.0.2.1|/expired") {
		t.Errorf("expired bucket survived DeleteExpiredTokenBuckets")
	}
	if !lmt.tokenBuckets.Contains("192.0.2.2|/live") {
		t.Errorf("live bucket was removed by DeleteExpiredTokenBuckets")
	}
}

// TestTokenBucketTTLResetUnchanged documents the pre-existing TTL semantics:
// the expiration is refreshed only when a bucket is (re)created, not on every
// hit, and capacity eviction must not interfere with that.
func TestTokenBucketTTLResetUnchanged(t *testing.T) {
	lmt := New(&ExpirableOptions{
		DefaultExpirationTTL: 300 * time.Millisecond,
		MaxTokenBuckets:      10,
	}).SetMax(100).SetBurst(1)

	key := "192.0.2.1|/ttl"
	lmt.LimitReached(key)

	expirationBefore, found := lmt.tokenBuckets.GetExpiration(key)
	if !found {
		t.Fatalf("bucket was not created")
	}

	time.Sleep(100 * time.Millisecond)
	lmt.LimitReached(key)

	expirationAfter, found := lmt.tokenBuckets.GetExpiration(key)
	if !found {
		t.Fatalf("bucket disappeared")
	}

	if !expirationAfter.Equal(expirationBefore) {
		t.Fatalf("a hit must not refresh the bucket expiration: before=%v after=%v",
			expirationBefore, expirationAfter)
	}
}
