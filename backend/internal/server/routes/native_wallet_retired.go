package routes

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

// Gateway balances were retired. Funding is owned by the console wallet.
func nativeWalletRetired(c *gin.Context) {
	c.AbortWithStatusJSON(http.StatusGone, gin.H{"code": "NATIVE_WALLET_RETIRED", "message": "Use the console USD wallet"})
}

func nativeBatchRetired(c *gin.Context) {
	c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": gin.H{"type": "native_batch_retired", "message": "Legacy batch image billing is retired; use canonical USD media tasks"}})
}
