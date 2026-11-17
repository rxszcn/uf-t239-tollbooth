package limiter

import (
	"time"
)

// DefaultMaxTokenBuckets is the default upper bound on the number of per-key
// token buckets a Limiter keeps in memory.
//
// The bound matters because rate-limit keys are derived from client-controlled
// input (forwarding headers, request paths, ...). Without it, a client rotating
// keys can force the limiter to allocate a bucket per key, each of which lives
// for DefaultExpirationTTL (10 years by default), which is a memory-exhaustion
// vector. At roughly 240 bytes per entry, this default caps a single limiter at
// roughly 24 MB while staying far above the number of distinct keys any
// realistic process sees within a token refill window.
const DefaultMaxTokenBuckets = 100_000

// ExpirableOptions are options used for new limiter creation
type ExpirableOptions struct {
	DefaultExpirationTTL time.Duration

	// MaxTokenBuckets caps how many per-key token buckets the limiter keeps in
	// memory. Once the cap is exceeded, the least recently used bucket is
	// evicted synchronously, while the bucket is being inserted; no sweep
	// goroutine is involved.
	//
	// 0 means "use DefaultMaxTokenBuckets". A negative value disables the bound,
	// which is only advisable if the set of rate-limit keys is known to be small
	// and closed.
	MaxTokenBuckets int

	// How frequently expire job triggers
	// Deprecated: not used anymore
	ExpireJobInterval time.Duration
}
