package middleware

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tracewayapp/lit/v2"
	"github.com/tracewayapp/traceway/backend/app/db"
	traceway "go.tracewayapp.com"
)

// RateLimitScope gives unrelated routes an explicit shared budget.
type RateLimitScope string

const OAuthTokenRateLimitScope RateLimitScope = "oauth-token"

const VerificationSendRateLimitScope RateLimitScope = "verification-send"

const pruneRateLimitBucketsEvery = 256

var rateLimitRequests atomic.Uint64

type databaseLimiter struct {
	maxRequests int
	window      time.Duration
	scope       RateLimitScope
}

// SharedFixedWindowLimiter applies a fixed-window budget through a caller-provided transaction.
type SharedFixedWindowLimiter struct {
	limiter *databaseLimiter
}

func NewSharedFixedWindowLimiter(scope RateLimitScope, maxRequests int, window time.Duration) *SharedFixedWindowLimiter {
	return &SharedFixedWindowLimiter{limiter: newDatabaseLimiter(scope, maxRequests, window)}
}

func (l *SharedFixedWindowLimiter) Allow(executor lit.Executor, key string) (bool, error) {
	allowed, _, err := l.limiter.allow(executor, string(l.limiter.scope), key)
	return allowed, err
}

func newDatabaseLimiter(scope RateLimitScope, maxRequests int, window time.Duration) *databaseLimiter {
	return &databaseLimiter{scope: scope, maxRequests: maxRequests, window: window}
}

func (l *databaseLimiter) allow(executor lit.Executor, scope, key string) (bool, time.Duration, error) {
	now := time.Now().UTC()
	expiresAt := now.Add(l.window).Unix()
	bucketKey := hashRateLimitKey(scope, key)

	query, args, err := lit.ParseNamedQuery(db.Driver, `
		INSERT INTO rate_limit_buckets (bucket_key, expires_at, request_count)
		VALUES (:bucket_key, :expires_at, 1)
		ON CONFLICT (bucket_key) DO UPDATE SET
			expires_at = CASE
				WHEN rate_limit_buckets.expires_at <= :now THEN excluded.expires_at
				ELSE rate_limit_buckets.expires_at
			END,
			request_count = CASE
				WHEN rate_limit_buckets.expires_at <= :now THEN 1
				ELSE rate_limit_buckets.request_count + 1
			END
		RETURNING request_count, expires_at`, lit.P{
		"bucket_key": bucketKey,
		"expires_at": expiresAt,
		"now":        now.Unix(),
	})
	if err != nil {
		return false, 0, err
	}

	var count int
	if err := executor.QueryRow(query, args...).Scan(&count, &expiresAt); err != nil {
		return false, 0, err
	}
	if rateLimitRequests.Add(1)%pruneRateLimitBucketsEvery == 0 {
		pruneExpiredRateLimitBuckets(executor, now.Unix())
	}

	retryAfter := time.Until(time.Unix(expiresAt, 0))
	if retryAfter < 0 {
		retryAfter = 0
	}
	return count <= l.maxRequests, retryAfter, nil
}

func hashRateLimitKey(scope, key string) string {
	sum := sha256.Sum256([]byte(scope + "\x00" + key))
	return hex.EncodeToString(sum[:])
}

func pruneExpiredRateLimitBuckets(executor lit.Executor, now int64) {
	query, args, err := lit.ParseNamedQuery(
		db.Driver,
		"DELETE FROM rate_limit_buckets WHERE expires_at <= :now",
		lit.P{"now": now},
	)
	if err != nil {
		traceway.CaptureException(traceway.NewStackTraceErrorf("prepare rate limit bucket cleanup: %w", err))
		return
	}
	if _, err := executor.Exec(query, args...); err != nil {
		traceway.CaptureException(traceway.NewStackTraceErrorf("prune rate limit buckets: %w", err))
	}
}

func rateLimitWithKey(scope RateLimitScope, maxRequests int, window time.Duration, keyOf func(c *gin.Context) string) gin.HandlerFunc {
	limiter := newDatabaseLimiter(scope, maxRequests, window)
	return func(c *gin.Context) {
		requestScope := string(limiter.scope)
		if requestScope == "" {
			requestScope = c.Request.Method + " " + c.FullPath()
		}
		allowed, retryAfter, err := limiter.allow(db.DB, requestScope, keyOf(c))
		if err != nil {
			traceway.CaptureException(traceway.NewStackTraceErrorf("rate limit %s: %w", requestScope, err))
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "temporarily_unavailable"})
			return
		}
		if !allowed {
			seconds := max(1, int(math.Ceil(retryAfter.Seconds())))
			c.Header("Retry-After", strconv.Itoa(seconds))
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "slow_down"})
			return
		}
		c.Next()
	}
}

// RateLimitPerIP returns a deployment-wide fixed-window limiter for one route.
func RateLimitPerIP(maxRequests int, window time.Duration) gin.HandlerFunc {
	return rateLimitWithKey("", maxRequests, window, func(c *gin.Context) string {
		return c.ClientIP()
	})
}

// RateLimitPerIPForScope shares one deployment-wide budget across multiple routes.
func RateLimitPerIPForScope(scope RateLimitScope, maxRequests int, window time.Duration) gin.HandlerFunc {
	return rateLimitWithKey(scope, maxRequests, window, func(c *gin.Context) string {
		return c.ClientIP()
	})
}

// RateLimitPerUser must run after UseAppAuth. Unresolved users fall back to IP.
func RateLimitPerUser(maxRequests int, window time.Duration) gin.HandlerFunc {
	return rateLimitWithKey("", maxRequests, window, func(c *gin.Context) string {
		if userId := GetUserId(c); userId != 0 {
			return "u:" + strconv.Itoa(userId)
		}
		return "ip:" + c.ClientIP()
	})
}
