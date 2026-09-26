package plugin

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sandstormmedia/nats/pkg/plugin/goja"
)

// startKVServer starts an in-process NATS server with JetStream and a "config" KV bucket.
func startKVServer(t *testing.T) *nats.Conn {
	t.Helper()
	ns, err := server.NewServer(&server.Options{Port: -1, JetStream: true, StoreDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	ns.Start()
	t.Cleanup(ns.Shutdown)
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server not ready")
	}

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)

	ctx := context.Background()
	js, _ := jetstream.New(nc)
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "config", History: 5})
	if err != nil {
		t.Fatal(err)
	}
	for _, put := range [][2]string{
		{"sensors.temperature", `{"celsius": 21.5}`},
		{"sensors.temperature", `{"celsius": 22.0}`},
		{"sensors.humidity", `{"percent": 40}`},
		{"mode", `eco`},
		{"gone", `x`},
	} {
		if _, err := kv.PutString(ctx, put[0], put[1]); err != nil {
			t.Fatal(err)
		}
	}
	if err := kv.Delete(ctx, "gone"); err != nil {
		t.Fatal(err)
	}
	return nc
}

func runKVQuery(t *testing.T, nc *nats.Conn, qm queryModel) *data.Frame {
	t.Helper()
	qm.RequestTimeout.Duration = 2 * time.Second
	res := (&Datasource{}).kv(context.Background(), qm, nc)
	if res.Error != nil {
		t.Fatal(res.Error)
	}
	return res.Frames[0]
}

func column(t *testing.T, frame *data.Frame, name string) []interface{} {
	t.Helper()
	field, _ := frame.FieldByName(name)
	if field == nil {
		t.Fatalf("no field %s in frame", name)
	}
	values := make([]interface{}, field.Len())
	for i := range values {
		v, _ := field.ConcreteAt(i)
		values[i] = v
	}
	return values
}

func assertJSON(t *testing.T, expected string, actual interface{}) {
	t.Helper()
	actualJSON, _ := json.Marshal(actual)
	if string(actualJSON) != expected {
		t.Errorf("expected %s, got %s", expected, actualJSON)
	}
}

func TestKVQueryLatestValues(t *testing.T) {
	nc := startKVServer(t)
	frame := runKVQuery(t, nc, queryModel{KvBucket: "config"})

	// deleted keys are skipped, only the latest revision is returned
	assertJSON(t, `["sensors.temperature","sensors.humidity","mode"]`, column(t, frame, "key"))
	assertJSON(t, `["{\"celsius\": 22.0}","{\"percent\": 40}","eco"]`, column(t, frame, "value"))
	assertJSON(t, `[2,3,4]`, column(t, frame, "revision"))
	assertJSON(t, `["PUT","PUT","PUT"]`, column(t, frame, "operation"))
}

func TestKVQueryFilterAndHistoryWithMapping(t *testing.T) {
	nc := startKVServer(t)
	frame := runKVQuery(t, nc, queryModel{
		KvBucket:  "config",
		KvKey:     "sensors.temperature",
		KvHistory: true,
		// 21.5 and 22.0 are exported as float64 and int64 by JS, so this also covers mixed number columns.
		JsFn: `return Object.assign({time: entry.created}, JSON.parse(entry.value));`,
	})

	assertJSON(t, `[21.5,22]`, column(t, frame, "celsius"))
	if len(column(t, frame, "time")) != 2 {
		t.Error("expected a time column with 2 rows")
	}
}

func TestKVQueryErrors(t *testing.T) {
	nc := startKVServer(t)
	qm := queryModel{KvBucket: "does-not-exist"}
	qm.RequestTimeout.Duration = 2 * time.Second
	if res := (&Datasource{}).kv(context.Background(), qm, nc); res.Status != backend.StatusBadRequest {
		t.Errorf("expected an error for a missing bucket, got %v", res)
	}

	frame := runKVQuery(t, nc, queryModel{KvBucket: "config", KvKey: "nothing.>"})
	if len(frame.Fields) != 0 {
		t.Errorf("expected an empty frame, got %d fields", len(frame.Fields))
	}
}

func TestKVInScript(t *testing.T) {
	nc := startKVServer(t)
	frame, err := goja.RunScript(nc, `
		const bucket = kv("config");
		const rows = [];
		for (const key of bucket.Keys("sensors.>")) {
			rows.push({key: key, value: bucket.Get(key).value});
		}
		rows.push({key: "missing", value: String(bucket.Get("missing"))});
		rows.push({key: "history", value: String(bucket.History("sensors.temperature").length)});
		rows.push({key: "entries", value: String(bucket.Entries("").length)});
		return rows;
	`, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	assertJSON(t, `["sensors.temperature","sensors.humidity","missing","history","entries"]`, column(t, frame, "key"))
	assertJSON(t, `["{\"celsius\": 22.0}","{\"percent\": 40}","null","2","3"]`, column(t, frame, "value"))

	if _, err := goja.RunScript(nc, `kv("does-not-exist")`, 2*time.Second); err == nil {
		t.Error("expected an error for a missing bucket")
	}
}
