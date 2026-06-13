package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rmrobinson/weather-server/internal/api"
	"github.com/rmrobinson/weather-server/internal/config"
	"github.com/rmrobinson/weather-server/internal/dashboard"
	"github.com/rmrobinson/weather-server/internal/grpcserver"
	"github.com/rmrobinson/weather-server/internal/hub"
	"github.com/rmrobinson/weather-server/internal/ingester"
	"github.com/rmrobinson/weather-server/internal/store"
	weatherv1 "github.com/rmrobinson/weather-server/proto/weather/v1"
	"go.uber.org/zap"
)

func main() {
	cfgPath := flag.String("config", "./config.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.LoadConfig(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}

	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "init logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	st, err := store.New(cfg.Influx, cfg.StationID, logger)
	if err != nil {
		logger.Fatal("init store", zap.Error(err))
	}
	if err := st.Bootstrap(ctx); err != nil {
		logger.Warn("store bootstrap failed (continuing)", zap.Error(err))
	}

	h := hub.New(logger)
	go h.Run(ctx)

	// Store subscribes to hub as "store-writer" (Option B: ingester decoupled from store).
	storeSub := h.Subscribe("store-writer")
	go func() {
		for {
			select {
			case r := <-storeSub.Ch:
				writeCtx, writeCancel := context.WithTimeout(ctx, 10*time.Second)
				err := st.WriteReading(writeCtx, r)
				writeCancel()
				if err != nil {
					logger.Error("write reading", zap.Error(err))
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	ing := ingester.New(cfg.MQTT, cfg.Latitude, cfg.Longitude, h, logger)
	go ing.Run(ctx)

	grpcSrv := grpcserver.NewGRPCServer(cfg.Auth.PSK)
	weatherv1.RegisterWeatherServiceServer(grpcSrv, grpcserver.New(h, st, logger))
	grpcLis, err := net.Listen("tcp", cfg.GRPC.Addr)
	if err != nil {
		logger.Fatal("grpc listen", zap.Error(err))
	}
	go func() {
		logger.Info("gRPC listening", zap.String("addr", cfg.GRPC.Addr))
		if err := grpcSrv.Serve(grpcLis); err != nil {
			logger.Error("grpc serve", zap.Error(err))
		}
	}()

	apiSrv := api.New(st, h, ing, cfg.Auth.PSK, logger)
	apiHandler := apiSrv.Handler()
	mux := http.NewServeMux()
	mux.Handle("/", dashboard.Handler())
	mux.Handle("/api/", apiHandler)
	mux.Handle("/healthz", apiHandler)

	httpSrv := &http.Server{
		Addr:    cfg.HTTP.Addr,
		Handler: mux,
		// ReadHeaderTimeout guards against slow-header attacks.
		// WriteTimeout is intentionally omitted: SSE connections are long-lived.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		logger.Info("HTTP listening", zap.String("addr", cfg.HTTP.Addr))
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http serve", zap.Error(err))
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()

	if err := httpSrv.Shutdown(shutCtx); err != nil {
		logger.Error("http shutdown", zap.Error(err))
	}
	// GracefulStop waits for active RPCs to finish. Force-stop anything still
	// open once the shutdown deadline expires so we don't block indefinitely
	// when streaming clients are connected.
	grpcDone := make(chan struct{})
	go func() {
		grpcSrv.GracefulStop()
		close(grpcDone)
	}()
	select {
	case <-grpcDone:
	case <-shutCtx.Done():
		grpcSrv.Stop()
	}
	logger.Info("shutdown complete")
}
