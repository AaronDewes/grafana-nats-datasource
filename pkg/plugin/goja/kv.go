package goja

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dop251/goja"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// KVEntryToMap converts a KV entry into a map which can be used from JS and rendered as a row.
// The value is converted to a string, like msg.Data.
func KVEntryToMap(entry jetstream.KeyValueEntry) map[string]interface{} {
	return map[string]interface{}{
		"key":       entry.Key(),
		"value":     string(entry.Value()),
		"revision":  int64(entry.Revision()),
		"created":   entry.Created(),
		"operation": kvOperation(entry.Operation()),
	}
}

// kvOperation returns the operation in the same notation as the KV-Operation message header.
func kvOperation(op jetstream.KeyValueOp) string {
	switch op {
	case jetstream.KeyValueDelete:
		return "DEL"
	case jetstream.KeyValuePurge:
		return "PURGE"
	default:
		return "PUT"
	}
}

// KVEntries returns the latest entry of every key matching filter (f.e. "foo.>"), or all revisions
// if includeHistory is set. Deleted keys are skipped.
func KVEntries(ctx context.Context, kv jetstream.KeyValue, filter string, includeHistory bool) ([]jetstream.KeyValueEntry, error) {
	if filter == "" {
		filter = ">"
	}
	opts := []jetstream.WatchOpt{jetstream.IgnoreDeletes()}
	if includeHistory {
		opts = append(opts, jetstream.IncludeHistory())
	}
	watcher, err := kv.Watch(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = watcher.Stop() }()

	entries := make([]jetstream.KeyValueEntry, 0)
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("reading KV entries: %w", ctx.Err())
		case entry := <-watcher.Updates():
			// nil marks the end of the initial values.
			if entry == nil {
				return entries, nil
			}
			entries = append(entries, entry)
		}
	}
}

// kvEntryToJSMap is like KVEntryToMap, but additionally contains the raw value as rawValue,
// which is converted to a Uint8Array by _wrapKVEntry in JS.
func kvEntryToJSMap(entry jetstream.KeyValueEntry) map[string]interface{} {
	m := KVEntryToMap(entry)
	m["rawValue"] = entry.Value()
	return m
}

// kvBucketW is the KV bucket API exposed to JS as kv("bucket"). Every call is bounded by timeout.
type kvBucketW struct {
	kv      jetstream.KeyValue
	timeout time.Duration
}

// Get returns the entry of key, or null if the key does not exist.
func (b *kvBucketW) Get(key string) (map[string]interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()
	entry, err := b.kv.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return kvEntryToJSMap(entry), nil
}

// Keys returns all keys matching filter (f.e. "foo.>"; empty means all keys).
func (b *kvBucketW) Keys(filter string) ([]interface{}, error) {
	if filter == "" {
		filter = ">"
	}
	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()
	lister, err := b.kv.ListKeysFiltered(ctx, filter)
	if err != nil {
		return nil, err
	}
	keys := make([]interface{}, 0)
	for key := range lister.Keys() {
		keys = append(keys, key)
	}
	return keys, nil
}

// Entries returns the latest entry of every key matching filter (empty means all keys).
func (b *kvBucketW) Entries(filter string) ([]interface{}, error) {
	return b.entries(filter, false)
}

// History returns all revisions of every key matching filter, oldest first.
func (b *kvBucketW) History(filter string) ([]interface{}, error) {
	return b.entries(filter, true)
}

func (b *kvBucketW) entries(filter string, includeHistory bool) ([]interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()
	entries, err := KVEntries(ctx, b.kv, filter, includeHistory)
	if err != nil {
		return nil, err
	}
	result := make([]interface{}, len(entries))
	for i, entry := range entries {
		result[i] = kvEntryToJSMap(entry)
	}
	return result, nil
}

// kvFn returns the kv("bucket") function exposed to JS.
func kvFn(nc *nats.Conn, timeout time.Duration) func(bucket string) (*kvBucketW, error) {
	return func(bucket string) (*kvBucketW, error) {
		js, err := jetstream.New(nc)
		if err != nil {
			return nil, err
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		kv, err := js.KeyValue(ctx, bucket)
		if err != nil {
			return nil, fmt.Errorf("KV bucket %s: %w", bucket, err)
		}
		return &kvBucketW{kv: kv, timeout: timeout}, nil
	}
}

// ConvertKVEntry runs jsFn for a single KV entry (available as "entry" in JS, with rawValue being the raw bytes)
// and returns the resulting row.
func ConvertKVEntry(nc *nats.Conn, entry map[string]interface{}, rawValue []byte, jsFn string) (map[string]interface{}, error) {
	vm := gojaPool.Get().(*goja.Runtime)
	defer gojaPool.Put(vm)
	if err := vm.Set("__nc", nc); err != nil {
		return nil, err
	}
	if err := vm.Set("__entry", entry); err != nil {
		return nil, err
	}
	if err := vm.Set("__entryRaw", rawValue); err != nil {
		return nil, err
	}
	// reset request-scoped variables - this way, we can have a clean VM again.
	defer func() {
		_ = vm.GlobalObject().Delete("__nc")
		_ = vm.GlobalObject().Delete("__entry")
		_ = vm.GlobalObject().Delete("__entryRaw")
	}()

	resultWrapper, err := vm.RunString(wrapJsKVEntry(jsFn))
	if err != nil {
		return nil, fmt.Errorf("could not run JS: %w  - JS was: %s", err, wrapJsKVEntry(jsFn))
	}

	row, ok := resultWrapper.Export().(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("result of KV entry script must be map[string]interface{}. Was: %T", resultWrapper.Export())
	}
	return row, nil
}

func wrapJsKVEntry(in string) string {
	return fmt.Sprintf(`
	"use strict";
	(function() {
		const {nats, nc} = _setup(_nats, _bytesToStr, _strToBytes, _parseDuration)(__nc, undefined);
		const entry = _wrapKVEntry(__entry, __entryRaw);
		%s;
    })()
`, in)
}
