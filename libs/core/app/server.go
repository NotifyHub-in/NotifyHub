package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/Arunshaik2001/notification-control-plane/libs/core/config"
	"github.com/Arunshaik2001/notification-control-plane/libs/core/httpx"
	"github.com/Arunshaik2001/notification-control-plane/libs/core/serviceinfo"
	"github.com/Arunshaik2001/notification-control-plane/libs/observability/metrics"
	obsruntime "github.com/Arunshaik2001/notification-control-plane/libs/observability/runtime"
)

type RouteRegistrar func(mux *http.ServeMux, info serviceinfo.Info, registry *metrics.Registry)

func RunHTTPService(cfg config.HTTPServiceConfig, logger *slog.Logger, registry *metrics.Registry, register RouteRegistrar) error {
	info := serviceinfo.Info{
		Name:        cfg.ServiceName,
		Version:     cfg.Version,
		Environment: cfg.Environment,
	}
	if registry == nil {
		registry = metrics.NewRegistry(cfg.ServiceName)
	}
	obsruntime.Register(registry, time.Now().UTC())

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("GET /info", func(w http.ResponseWriter, _ *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, info)
	})
	mux.Handle("GET /metrics", registry.Handler())

	if register != nil {
		register(mux, info, registry)
	}

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           observabilityMiddleware(logger, registry, mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	logger.Info("starting service", "service", cfg.ServiceName, "port", cfg.Port)

	err := server.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return server.Shutdown(ctx)
}

func observabilityMiddleware(logger *slog.Logger, registry *metrics.Registry, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(recorder, r)

		pattern := r.Pattern
		if pattern == "" {
			pattern = r.URL.Path
		}

		labels := map[string]string{
			"service": registry.Service(),
			"method":  r.Method,
			"path":    pattern,
			"status":  fmt.Sprintf("%d", recorder.statusCode),
		}
		registry.IncCounter("http_requests_total", "Total HTTP requests handled.", labels)
		registry.ObserveHistogram("http_request_duration_seconds", "HTTP request duration in seconds.", labels, metrics.DefaultLatencyBuckets(), time.Since(start).Seconds())

		logger.Info("request completed", "method", r.Method, "path", r.URL.Path, "pattern", pattern, "status", recorder.statusCode, "duration", time.Since(start))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *statusRecorder) WriteHeader(statusCode int) {
	r.statusCode = statusCode
	r.ResponseWriter.WriteHeader(statusCode)
}
