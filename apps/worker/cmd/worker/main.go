package main

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/your-org/notification-control-plane/libs/core/app"
	"github.com/your-org/notification-control-plane/libs/core/config"
	"github.com/your-org/notification-control-plane/libs/core/httpx"
	"github.com/your-org/notification-control-plane/libs/core/serviceinfo"
	"github.com/your-org/notification-control-plane/libs/observability/logging"
)

func main() {
	cfg, err := config.LoadHTTPServiceConfig("worker", 8081)
	if err != nil {
		panic(err)
	}

	logger := logging.New(cfg.ServiceName)
	var lastHeartbeat sync.Map

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-ticker.C:
				lastHeartbeat.Store("reconciler", t.UTC().Format(time.RFC3339))
				logger.Info("worker heartbeat", "component", "reconciler", "time", t.UTC().Format(time.RFC3339))
			}
		}
	}()

	err = app.RunHTTPService(cfg, logger, func(mux *http.ServeMux, info serviceinfo.Info) {
		mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, _ *http.Request) {
			heartbeat, _ := lastHeartbeat.Load("reconciler")
			httpx.WriteJSON(w, http.StatusOK, map[string]any{
				"service":        info.Name,
				"phase":          "scaffold",
				"state":          "worker loop active",
				"last_heartbeat": heartbeat,
			})
		})
	})
	if err != nil {
		panic(err)
	}
}
