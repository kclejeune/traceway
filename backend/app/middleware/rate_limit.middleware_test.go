package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tracewayapp/traceway/backend/app/db"
	"github.com/tracewayapp/traceway/backend/app/dbtest"
)

func TestRateLimitPerIPSharesBucketsAcrossMiddlewareInstances(t *testing.T) {
	dbtest.SetupSQLite(t)
	gin.SetMode(gin.TestMode)

	first := gin.New()
	first.GET("/login", RateLimitPerIP(2, time.Minute), func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})
	second := gin.New()
	second.GET("/login", RateLimitPerIP(2, time.Minute), func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	for i, router := range []*gin.Engine{first, second, first} {
		response := performRateLimitedRequest(router, "/login", "192.0.2.10:1234")
		if i < 2 && response.Code != http.StatusNoContent {
			t.Fatalf("request %d returned %d, want %d", i+1, response.Code, http.StatusNoContent)
		}
		if i == 2 {
			if response.Code != http.StatusTooManyRequests {
				t.Fatalf("request %d returned %d, want %d", i+1, response.Code, http.StatusTooManyRequests)
			}
			if response.Header().Get("Retry-After") == "" {
				t.Fatal("throttled response is missing Retry-After")
			}
		}
	}

	var bucketKey string
	if err := db.DB.QueryRow("SELECT bucket_key FROM rate_limit_buckets").Scan(&bucketKey); err != nil {
		t.Fatalf("read rate limit bucket: %v", err)
	}
	if bucketKey == "192.0.2.10" {
		t.Fatal("rate limit bucket stored the unhashed client IP")
	}

	if _, err := db.DB.Exec("UPDATE rate_limit_buckets SET expires_at = 0"); err != nil {
		t.Fatalf("expire rate limit bucket: %v", err)
	}
	afterExpiry := performRateLimitedRequest(second, "/login", "192.0.2.10:1234")
	if afterExpiry.Code != http.StatusNoContent {
		t.Fatalf("request after expiry returned %d, want %d", afterExpiry.Code, http.StatusNoContent)
	}
	var count int
	if err := db.DB.QueryRow("SELECT request_count FROM rate_limit_buckets").Scan(&count); err != nil {
		t.Fatalf("read reset rate limit bucket: %v", err)
	}
	if count != 1 {
		t.Fatalf("request count after expiry = %d, want 1", count)
	}
}

func TestRateLimitPerIPForScopeSharesBudgetAcrossRoutes(t *testing.T) {
	dbtest.SetupSQLite(t)
	gin.SetMode(gin.TestMode)

	router := gin.New()
	limit := RateLimitPerIPForScope(OAuthTokenRateLimitScope, 1, time.Minute)
	router.POST("/auth/device/token", limit, func(c *gin.Context) { c.Status(http.StatusNoContent) })
	router.POST("/auth/token", limit, func(c *gin.Context) { c.Status(http.StatusNoContent) })

	first := performRateLimitedRequestWithMethod(router, http.MethodPost, "/auth/device/token", "192.0.2.20:1234")
	if first.Code != http.StatusNoContent {
		t.Fatalf("first route returned %d, want %d", first.Code, http.StatusNoContent)
	}
	second := performRateLimitedRequestWithMethod(router, http.MethodPost, "/auth/token", "192.0.2.20:1234")
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("second route returned %d, want %d", second.Code, http.StatusTooManyRequests)
	}
}

func performRateLimitedRequest(router http.Handler, path, remoteAddr string) *httptest.ResponseRecorder {
	return performRateLimitedRequestWithMethod(router, http.MethodGet, path, remoteAddr)
}

func performRateLimitedRequestWithMethod(router http.Handler, method, path, remoteAddr string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, nil)
	request.RemoteAddr = remoteAddr
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	return response
}
