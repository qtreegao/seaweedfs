package weed_server

import (
	"testing"

	dto "github.com/prometheus/client_model/go"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

func gaugeValue(t *testing.T, g interface{ Write(*dto.Metric) error }) float64 {
	t.Helper()
	m := &dto.Metric{}
	if err := g.Write(m); err != nil {
		t.Fatal(err)
	}
	return m.GetGauge().GetValue()
}

func TestUpdateSubscribeProgressLocalAndAggregate(t *testing.T) {
	stats.FilerSubscribeSourceLatestTsGauge.Reset()
	stats.FilerSubscribeDeliveredTsGauge.Reset()
	stats.FilerSubscribeBacklogTsGauge.Reset()
	t.Cleanup(func() {
		stats.FilerSubscribeSourceLatestTsGauge.Reset()
		stats.FilerSubscribeDeliveredTsGauge.Reset()
		stats.FilerSubscribeBacklogTsGauge.Reset()
	})

	f := &filer.Filer{LocalMetaLogBuffer: log_buffer.NewLogBuffer("test", filer.LogFlushInterval, nil, nil, nil)}
	fs := &FilerServer{filer: f, option: &FilerOption{Host: pb.ServerAddress("source:8888")}}
	req := &filer_pb.SubscribeMetadataRequest{ClientName: "client", PathPrefix: "/data"}
	if err := f.LocalMetaLogBuffer.AddDataToBuffer([]byte("/data"), []byte("x"), 900); err != nil {
		t.Fatal(err)
	}
	fs.updateSubscribeProgress(req, 400, false)
	labels := []string{"source:8888", "client", "/data"}
	if got := gaugeValue(t, stats.FilerSubscribeSourceLatestTsGauge.WithLabelValues(labels...)); got != 900 {
		t.Fatalf("local source latest = %v, want 900", got)
	}
	if got := gaugeValue(t, stats.FilerSubscribeDeliveredTsGauge.WithLabelValues(labels...)); got != 400 {
		t.Fatalf("local delivered = %v, want 400", got)
	}
	if got := gaugeValue(t, stats.FilerSubscribeBacklogTsGauge.WithLabelValues(labels...)); got != 500 {
		t.Fatalf("local backlog = %v, want 500", got)
	}

	f.MetaAggregator = filer.NewMetaAggregator(f, pb.ServerAddress("self:8888"), nil)
	if err := f.MetaAggregator.MetaLogBuffer.AddDataToBuffer([]byte("/data"), []byte("x"), 1200); err != nil {
		t.Fatal(err)
	}
	fs.updateSubscribeProgress(req, 1000, true)
	if got := gaugeValue(t, stats.FilerSubscribeSourceLatestTsGauge.WithLabelValues(labels...)); got != 1200 {
		t.Fatalf("aggregate source latest = %v, want 1200", got)
	}
	if got := gaugeValue(t, stats.FilerSubscribeBacklogTsGauge.WithLabelValues(labels...)); got != 200 {
		t.Fatalf("aggregate backlog = %v, want 200", got)
	}
}
