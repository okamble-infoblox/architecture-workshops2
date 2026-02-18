package cases

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/infobloxopen/architecture-workshops2/pkg/depclient"
)

// TxCase handles Case 2: DB transaction scope anti-pattern.
type TxCase struct {
	DB        *sql.DB
	DepClient *depclient.Client
}

// Handle serves the /cases/tx endpoint.
// BASELINE (broken): Holds a database transaction open while making
// a slow network call to the dep service. This causes connection pool
// exhaustion under load.
func (tc *TxCase) Handle(w http.ResponseWriter, r *http.Request) {
	if tc.DB == nil {
		http.Error(w, "database not configured", http.StatusServiceUnavailable)
		return
	}

	start := time.Now()

	// LAB: STEP2 TODO - This is the anti-pattern: BEGIN TX, then make a
	// slow network call while holding the transaction open.
	// The fix is to:
	//   1. Move the dep call OUTSIDE the transaction
	//   2. Only use the TX for the actual DB operation
	//   3. Keep TX duration as short as possible

	// LAB: STEP2 FIXED - Make the network call BEFORE starting the transaction
	// This way we don't hold DB connections or row locks during the slow network call
	_, depErr := depclient.Call(r.Context(), tc.DepClient, "2s", "0.0")
	if depErr != nil {
		log.Printf("tx: dep call error: %v", depErr)
	}

	// Begin transaction - keep it as short as possible
	tx, err := tc.DB.Begin()
	if err != nil {
		log.Printf("tx: begin error: %v", err)
		http.Error(w, "tx begin failed", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	// LAB: STEP2 FIXED - Lock a row inside the transaction, but now only for the minimal time
	// This SELECT FOR UPDATE holds a row lock only during the actual DB operations
	var balance int
	err = tx.QueryRow("SELECT balance FROM accounts WHERE name = $1 FOR UPDATE", "alice").Scan(&balance)
	if err != nil {
		log.Printf("tx: query error: %v", err)
		http.Error(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Update the row
	_, err = tx.Exec("UPDATE accounts SET balance = balance - 1, updated_at = NOW() WHERE name = $1", "alice")
	if err != nil {
		log.Printf("tx: update error: %v", err)
		http.Error(w, "update failed", http.StatusInternalServerError)
		return
	}

	// Commit
	if err := tx.Commit(); err != nil {
		log.Printf("tx: commit error: %v", err)
		http.Error(w, "commit failed", http.StatusInternalServerError)
		return
	}

	elapsed := time.Since(start)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":     "ok",
		"balance":    balance,
		"elapsed_ms": elapsed.Milliseconds(),
	})
}
