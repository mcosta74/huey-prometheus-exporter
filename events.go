package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

type metrics struct {
	info       prometheus.Gauge
	executions *prometheus.CounterVec
	completed  *prometheus.CounterVec
	locked     *prometheus.CounterVec
	duration   *prometheus.HistogramVec
}

func setupMetrics(prefix string) *metrics {
	m := &metrics{
		info: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: prefix,
			Subsystem: "scheduler",
			Name:      "build_info",
			Help:      "Build information.",
			ConstLabels: prometheus.Labels{
				"Version": "v0.1.0",
				"Author":  "Massimo Costa <costa.massimo@gmail.com>",
			},
		}),
		executions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: prefix,
			Subsystem: "scheduler",
			Name:      "task_execution_total",
			Help:      "The Number of times a scheduler task has been executed.",
		}, []string{"task_name"}),
		completed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: prefix,
			Subsystem: "scheduler",
			Name:      "task_completed_total",
			Help:      "The Number of times a scheduler task has been completed.",
		}, []string{"task_name", "success"}),
		locked: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: prefix,
			Subsystem: "scheduler",
			Name:      "task_locked_total",
			Help:      "The Number of times a scheduler task failed to acquire a lock.",
		}, []string{"task_name"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: prefix,
			Subsystem: "scheduler",
			Name:      "task_duration_seconds",
			Help:      "Task duration in seconds.",
		}, []string{"task_name", "success"}),
	}

	prometheus.MustRegister(m.info)
	prometheus.MustRegister(m.executions)
	prometheus.MustRegister(m.completed)
	prometheus.MustRegister(m.locked)
	prometheus.MustRegister(m.duration)
	return m
}

type EventListener interface {
	Run(ctx context.Context) error
}

type listener struct {
	rdb               *redis.Client
	channel           string
	logger            log.Logger
	m                 *metrics
	startExecutionMap map[string]time.Time
}

func NewEventListener(rdb *redis.Client, channel string, logger log.Logger, prefix string) EventListener {
	return &listener{
		rdb:               rdb,
		channel:           channel,
		logger:            logger,
		m:                 setupMetrics(prefix),
		startExecutionMap: make(map[string]time.Time),
	}
}

func (l *listener) Run(ctx context.Context) error {
	pubsub := l.rdb.Subscribe(ctx, l.channel)
	_, err := pubsub.Receive(ctx)
	if err != nil {
		return err
	}

	ch := pubsub.Channel()

	for {
		select {
		case evt := <-ch:
			if err = l.handleEvent(evt); err != nil {
				level.Error(l.logger).Log("during", "handleEvent", "err", err)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (l *listener) handleEvent(msg *redis.Message) error {
	var evt event
	if err := json.Unmarshal([]byte(msg.Payload), &evt); err != nil {
		return err
	}

	level.Debug(l.logger).Log("event", fmt.Sprintf("%+v", evt))

	switch evt.Event {
	case SIGNAL_EXECUTING:
		l.m.executions.WithLabelValues(evt.TaskName).Inc()
		l.startExecutionMap[evt.TaskId] = time.Now()

	case SIGNAL_COMPLETE, SIGNAL_ERROR:
		labelValues := []string{evt.TaskName, fmt.Sprint(evt.Event == SIGNAL_COMPLETE)}
		l.m.completed.WithLabelValues(labelValues...).Inc()
		if startTime, ok := l.startExecutionMap[evt.TaskId]; ok {
			l.m.duration.WithLabelValues(labelValues...).Observe(time.Since(startTime).Seconds())
			delete(l.startExecutionMap, evt.TaskId)
		}

	case SIGNAL_LOCKED:
		l.m.locked.WithLabelValues(evt.TaskName).Inc()
	}
	return nil
}

type eventType string

const (
	SIGNAL_CANCELED    eventType = "canceled"
	SIGNAL_COMPLETE    eventType = "complete"
	SIGNAL_ERROR       eventType = "error"
	SIGNAL_EXECUTING   eventType = "executing"
	SIGNAL_EXPIRED     eventType = "expired"
	SIGNAL_LOCKED      eventType = "locked"
	SIGNAL_RETRYING    eventType = "retrying"
	SIGNAL_REVOKED     eventType = "revoked"
	SIGNAL_SCHEDULED   eventType = "scheduled"
	SIGNAL_INTERRUPTED eventType = "interrupted"
)

type event struct {
	Event    eventType `json:"event,omitempty"`
	TaskName string    `json:"task_name,omitempty"`
	TaskId   string    `json:"task_id,omitempty"`
}
