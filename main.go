package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"syscall"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/run"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

func getEnv(key, defaultVal string) string {
	if val, found := os.LookupEnv(key); found {
		return val
	}
	return defaultVal
}

func main() {
	var (
		redisAddr     = flag.String("redis.addr", getEnv("HUEY_EXPORTER_REDIS_ADDR", "localhost:6379"), "Address of the Redis instance")
		redisChannel  = flag.String("redis.channel", getEnv("HUEY_EXPORTER_REDIS_CHANNEL", "fw:events"), "Channel to subscribe to")
		listenAddr    = flag.String("web.listen-address", getEnv("HUEY_EXPORTER_WEB_LISTEN_ADDRESS", ":9132"), "Address to listen on for web interface and telemetry.")
		logLevel      = flag.String("log-level", getEnv("HUEY_EXPORTER_LOG_LEVEL", "info"), `Log level ("error", "warning", "info", "debug").`)
		logFormat     = flag.String("log-format", getEnv("HUEY_EXPORTER_LOG_FORMAT", "text"), `Log format ("text", "json").`)
		metricsPrefix = flag.String("metrics-prefix", getEnv("HUEY_EXPORTER_METRICS_PREFIX", ""), `Prefix to be used for generates metrics.`)
	)
	flag.Parse()

	var logger log.Logger
	{
		logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
		if *logFormat == "json" {
			logger = log.NewJSONLogger(log.NewSyncWriter(os.Stderr))
		}
		logger = level.NewFilter(logger, level.Allow(level.ParseDefault(*logLevel, level.InfoValue())))
		logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)
	}
	level.Info(logger).Log("msg", "Process started")

	var g run.Group
	{
		// Signal Handler
		g.Add(run.SignalHandler(context.Background(), syscall.SIGTERM, syscall.SIGINT))
	}

	{
		// Prometheus HTTP
		logger = log.With(logger, "transport", "HTTP")
		webListener, err := net.Listen("tcp", *listenAddr)
		if err != nil {
			level.Error(logger).Log("during", "Listen()", "err", err)
			os.Exit(1)
		}

		g.Add(func() error {
			level.Info(logger).Log("addr", fmt.Sprintf("http://%s/metric", webListener.Addr()))
			http.Handle("/metrics", promhttp.Handler())
			return http.Serve(webListener, http.DefaultServeMux)
		}, func(err error) {
			webListener.Close()
		})
	}

	{
		logger = log.With(logger, "transport", "Redis")
		rdb := redis.NewClient(&redis.Options{
			Addr: *redisAddr,
		})

		ctx := context.Background()
		_, err := rdb.Ping(ctx).Result()
		if err != nil {
			level.Error(logger).Log("during", "Ping()", "err", err)
			os.Exit(1)
		}

		ctx, cancel := context.WithCancel(ctx)
		g.Add(func() error {
			service := NewEventListener(rdb, *redisChannel, logger, *metricsPrefix)
			return service.Run(ctx)
		}, func(err error) {
			cancel()
		})
	}

	level.Info(logger).Log("msg", "Process exit", "err", g.Run())
}
