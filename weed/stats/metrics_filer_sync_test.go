package stats

import (
	"testing"

	dto "github.com/prometheus/client_model/go"
)

func TestFilerSyncMetricsRegisteredAndValues(t *testing.T) {
	FilerSyncPendingTasksGauge.Reset()
	FilerSyncLagSecondsGauge.Reset()
	FilerSyncReceivedCounter.Reset()
	FilerSyncProcessedCounter.Reset()
	FilerSyncFailedCounter.Reset()
	t.Cleanup(func() {
		FilerSyncPendingTasksGauge.Reset()
		FilerSyncLagSecondsGauge.Reset()
		FilerSyncReceivedCounter.Reset()
		FilerSyncProcessedCounter.Reset()
		FilerSyncFailedCounter.Reset()
	})

	labels := []string{"source:8888", "target:8888", "syncFrom_source_To_target", "/data"}
	wantLabels := map[string]string{
		"sourceFiler": labels[0],
		"targetFiler": labels[1],
		"clientName":  labels[2],
		"path":        labels[3],
	}
	FilerSyncPendingTasksGauge.WithLabelValues(labels...).Set(7)
	FilerSyncLagSecondsGauge.WithLabelValues(labels...).Set(3.5)
	FilerSyncReceivedCounter.WithLabelValues(labels...).Add(11)
	FilerSyncProcessedCounter.WithLabelValues(labels...).Add(9)
	FilerSyncFailedCounter.WithLabelValues(labels...).Add(2)

	families, err := Gather.Gather()
	if err != nil {
		t.Fatalf("gather filer.sync metrics: %v", err)
	}

	expected := map[string]struct {
		value float64
		kind  string
	}{
		"SeaweedFS_filerSync_pending_tasks":   {value: 7, kind: "gauge"},
		"SeaweedFS_filerSync_lag_seconds":     {value: 3.5, kind: "gauge"},
		"SeaweedFS_filerSync_received_total":  {value: 11, kind: "counter"},
		"SeaweedFS_filerSync_processed_total": {value: 9, kind: "counter"},
		"SeaweedFS_filerSync_failed_total":    {value: 2, kind: "counter"},
	}

	for name, want := range expected {
		family := findMetricFamily(families, name)
		if family == nil {
			t.Fatalf("metric family %s was not registered", name)
		}
		if len(family.Metric) != 1 {
			t.Fatalf("metric family %s has %d samples, want 1", name, len(family.Metric))
		}
		metric := family.Metric[0]
		var got float64
		switch want.kind {
		case "gauge":
			if metric.Gauge == nil {
				t.Fatalf("metric %s is not a gauge", name)
			}
			got = metric.Gauge.GetValue()
		case "counter":
			if metric.Counter == nil {
				t.Fatalf("metric %s is not a counter", name)
			}
			got = metric.Counter.GetValue()
		}
		if got != want.value {
			t.Errorf("metric %s = %v, want %v", name, got, want.value)
		}
		if len(metric.Label) != len(labels) {
			t.Errorf("metric %s has %d labels, want %d", name, len(metric.Label), len(labels))
		}
		seenLabels := make(map[string]bool, len(metric.Label))
		for _, label := range metric.Label {
			wantValue, ok := wantLabels[label.GetName()]
			if !ok {
				t.Errorf("metric %s has unexpected label %q", name, label.GetName())
			} else if label.GetValue() != wantValue {
				t.Errorf("metric %s label %s = %q, want %q", name, label.GetName(), label.GetValue(), wantValue)
			}
			seenLabels[label.GetName()] = true
		}
		for labelName := range wantLabels {
			if !seenLabels[labelName] {
				t.Errorf("metric %s is missing label %q", name, labelName)
			}
		}
	}
}

func TestFilerSubscribeProgressMetrics(t *testing.T) {
	FilerSubscribeSourceLatestTsGauge.Reset()
	FilerSubscribeDeliveredTsGauge.Reset()
	FilerSubscribeBacklogTsGauge.Reset()
	t.Cleanup(func() {
		FilerSubscribeSourceLatestTsGauge.Reset()
		FilerSubscribeDeliveredTsGauge.Reset()
		FilerSubscribeBacklogTsGauge.Reset()
	})
	labels := []string{"source:8888", "sync-client", "/data"}
	FilerSubscribeSourceLatestTsGauge.WithLabelValues(labels...).Set(900)
	FilerSubscribeDeliveredTsGauge.WithLabelValues(labels...).Set(400)
	FilerSubscribeBacklogTsGauge.WithLabelValues(labels...).Set(500)
	families, err := Gather.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]float64{
		"SeaweedFS_filer_subscribe_source_latest_ts_ns": 900,
		"SeaweedFS_filer_subscribe_delivered_ts_ns":     400,
		"SeaweedFS_filer_subscribe_backlog_ts_ns":       500,
	} {
		family := findMetricFamily(families, name)
		if family == nil || len(family.Metric) != 1 || family.Metric[0].Gauge == nil {
			t.Fatalf("metric family %s missing or wrong type", name)
		}
		if got := family.Metric[0].Gauge.GetValue(); got != want {
			t.Errorf("metric %s = %v, want %v", name, got, want)
		}
	}
}

func findMetricFamily(families []*dto.MetricFamily, name string) *dto.MetricFamily {
	for _, family := range families {
		if family.GetName() == name {
			return family
		}
	}
	return nil
}
