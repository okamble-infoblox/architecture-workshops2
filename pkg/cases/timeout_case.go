package cases

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/infobloxopen/architecture-workshops2/pkg/depclient"
)

// TimeoutCase handles Case 1: calling a slow dependency without proper timeouts.
type TimeoutCase struct {
	DepClient *depclient.Client
}

// Handle serves the /cases/timeouts endpoint.
// BASELINE (broken): No timeout, no context deadline.
// The dep service is configured to sleep for 3-5s, so requests pile up.
func (tc *TimeoutCase) Handle(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	// LAB: STEP1 FIXED - Add context with timeout to prevent hanging
	// Set a 4-second deadline to allow the 3s dep call to complete while preventing hangs
	ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
	defer cancel()

	// Call dep service with a slow sleep parameter
	result, err := depclient.Call(ctx, tc.DepClient, "3s", "0.0")
	elapsed := time.Since(start)

	if err != nil {
		log.Printf("timeouts: error after %v: %v", elapsed, err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusGatewayTimeout)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error":      err.Error(),
			"elapsed_ms": elapsed.Milliseconds(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":     "ok",
		"dep_result": result,
		"elapsed_ms": elapsed.Milliseconds(),
	})
	_ = fmt.Sprintf("timeouts: completed in %v", elapsed)
}
