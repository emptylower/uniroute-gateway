package middleware

import (
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// Bound unaffordable polling work independently of financial usage limits.
func PublicMediaRateLimit() gin.HandlerFunc {
	type window struct {
		start          time.Time
		reads, creates int
	}
	var mu sync.Mutex
	windows := map[int64]window{}
	var nextSweep time.Time
	return func(c *gin.Context) {
		key, ok := GetAPIKeyFromContext(c)
		if !ok {
			c.Abort()
			return
		}
		now := time.Now()
		mu.Lock()
		w := windows[key.ID]
		if now.Sub(w.start) >= time.Minute {
			w = window{start: now}
		}
		create := c.Request.Method == http.MethodPost && c.Request.URL.Path == "/v1/media/tasks"
		rejected := w.reads >= 120 || (create && w.creates >= 10)
		if !rejected {
			w.reads++
			if create {
				w.creates++
			}
			windows[key.ID] = w
		}
		if len(windows) > 4096 && !now.Before(nextSweep) {
			nextSweep = now.Add(time.Minute)
			for id, old := range windows {
				if now.Sub(old.start) >= time.Minute {
					delete(windows, id)
				}
			}
		}
		// Bound cardinality even under credential churn.
		if len(windows) > 8192 {
			delete(windows, key.ID)
			rejected = true
		}
		mu.Unlock()
		if rejected {
			c.Header("Retry-After", "60")
			c.JSON(429, gin.H{"code": 429, "message": "media request rate exceeded", "reason": "MEDIA_RATE_LIMITED", "data": nil})
			c.Abort()
			return
		}
		c.Next()
	}
}
