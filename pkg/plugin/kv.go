package plugin

import (
	"context"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sandstormmedia/nats/pkg/plugin/framestruct"
	"github.com/sandstormmedia/nats/pkg/plugin/goja"
)

// kv reads the entries of a JetStream Key/Value bucket, one row per entry.
func (ds *Datasource) kv(ctx context.Context, qm queryModel, nc *nats.Conn) backend.DataResponse {
	if qm.KvBucket == "" {
		return backend.ErrDataResponse(backend.StatusBadRequest, "KV bucket must be specified")
	}

	ctx, cancel := context.WithTimeout(ctx, qm.RequestTimeout.Duration)
	defer cancel()

	js, err := jetstream.New(nc)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, "JetStream error: "+err.Error())
	}
	store, err := js.KeyValue(ctx, qm.KvBucket)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, "KV bucket "+qm.KvBucket+": "+err.Error())
	}
	entries, err := goja.KVEntries(ctx, store, qm.KvKey, qm.KvHistory)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, "KV bucket "+qm.KvBucket+": "+err.Error())
	}

	rows := make([]map[string]interface{}, 0, len(entries))
	for _, entry := range entries {
		row := goja.KVEntryToMap(entry)
		if qm.JsFn != "" {
			row, err = goja.ConvertKVEntry(nc, row, qm.JsFn)
			if err != nil {
				return backend.ErrDataResponse(backend.StatusBadRequest, "could not convert KV entry "+entry.Key()+": "+err.Error())
			}
		}
		rows = append(rows, row)
	}

	frame := data.NewFrame("result")
	if len(rows) > 0 {
		frame, err = framestruct.ToDataFrame("result", rows)
		if err != nil {
			return backend.ErrDataResponse(backend.StatusBadRequest, "Response conversion error: "+err.Error())
		}
	}

	return backend.DataResponse{
		Frames: data.Frames{frame},
		Status: backend.StatusOK,
	}
}
