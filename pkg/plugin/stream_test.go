package plugin

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/nats-io/nats.go"
	"github.com/sandstormmedia/nats/pkg/plugin/goja"
)

// newTestDatasource builds a datasource the same way Grafana does, but with a short stream TTL so
// that expiry can be observed.
func newTestDatasource(t *testing.T, ttl time.Duration) *Datasource {
	t.Helper()
	instance, err := NewDatasource(context.Background(), backend.DataSourceInstanceSettings{UID: "test"})
	if err != nil {
		t.Fatal(err)
	}
	ds := instance.(*Datasource)
	ds.streamTTL = ttl
	t.Cleanup(ds.Dispose)
	return ds
}

// subscribeQuery builds a SUBSCRIBE query. timeout is how long the query waits for the first
// message, and must stay well below the stream TTL, or the stream expires before it is handed over.
func subscribeQuery(subject string, uuid string, timeout time.Duration) queryModel {
	qm := queryModel{
		QueryType:                   QueryTypeSubscribe,
		NatsSubject:                 subject,
		StreamRequestUuidForTesting: uuid,
		JsFn:                        `return {value: 1};`,
	}
	qm.RequestTimeout.Duration = timeout
	return qm
}

// waitFor polls until condition holds, so the tests do not depend on fixed sleeps.
func waitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// A quiet subject must not block the query: it used to wait for the first message forever, leaking
// the goroutine and the NATS subscription.
func TestSubscribeDoesNotBlockOnQuietSubject(t *testing.T) {
	nc := startNatsServer(t)
	ds := newTestDatasource(t, time.Minute)

	start := time.Now()
	response := ds.subscribe(context.Background(), subscribeQuery("quiet.subject", "uuid-quiet", 200*time.Millisecond), nc)
	elapsed := time.Since(start)

	if response.Error != nil {
		t.Fatalf("expected a streaming response, got %v", response.Error)
	}
	if elapsed > 2*time.Second {
		t.Errorf("subscribe took %s - it should give up waiting after the request timeout", elapsed)
	}
	// the panel still gets a live channel, so it fills up once messages arrive.
	if len(response.Frames) != 1 || response.Frames[0].Meta == nil || response.Frames[0].Meta.Channel == "" {
		t.Error("expected an empty frame carrying the live channel")
	}
}

// A cancelled query must not leave the subscription behind.
func TestSubscribeHonoursCancelledQuery(t *testing.T) {
	nc := startNatsServer(t)
	ds := newTestDatasource(t, time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	qm := subscribeQuery("cancelled.subject", "uuid-cancelled", 10*time.Second)

	response := ds.subscribe(ctx, qm, nc)
	if response.Error == nil {
		t.Error("expected an error for a cancelled query")
	}
	if ds.streamResponsesSoFar.Len() != 0 {
		t.Errorf("expected no cached stream, got %d", ds.streamResponsesSoFar.Len())
	}
	waitFor(t, "the subscription to be closed", func() bool { return nc.NumSubscriptions() == 0 })
}

// An abandoned stream must be evicted from the cache and unsubscribed. Before the fix the cache
// never removed expired entries and the subscription stayed open for the life of the process.
func TestAbandonedStreamIsEvictedAndUnsubscribed(t *testing.T) {
	nc := startNatsServer(t)
	ds := newTestDatasource(t, 300*time.Millisecond)

	if response := ds.subscribe(context.Background(), subscribeQuery("abandoned.subject", "uuid-abandoned", 20*time.Millisecond), nc); response.Error != nil {
		t.Fatal(response.Error)
	}
	if ds.streamResponsesSoFar.Len() != 1 {
		t.Fatalf("expected the stream to be cached, got %d entries", ds.streamResponsesSoFar.Len())
	}

	waitFor(t, "the expired stream to be evicted", func() bool { return ds.streamResponsesSoFar.Len() == 0 })
	waitFor(t, "the subscription to be closed", func() bool { return nc.NumSubscriptions() == 0 })
}

// Repeated queries, as a dashboard refresh produces them, must not pile up.
func TestRepeatedSubscribeQueriesDoNotPileUp(t *testing.T) {
	nc := startNatsServer(t)
	ds := newTestDatasource(t, 300*time.Millisecond)

	for i := 0; i < 20; i++ {
		if response := ds.subscribe(context.Background(), subscribeQuery("refresh.subject", fmt.Sprintf("uuid-%d", i), 20*time.Millisecond), nc); response.Error != nil {
			t.Fatal(response.Error)
		}
	}

	waitFor(t, "all streams to be evicted", func() bool { return ds.streamResponsesSoFar.Len() == 0 })
	waitFor(t, "all subscriptions to be closed", func() bool { return nc.NumSubscriptions() == 0 })
}

// When the panel goes away, RunStream must drop the entry and end the subscription.
func TestRunStreamReleasesEverythingOnCancel(t *testing.T) {
	nc := startNatsServer(t)
	ds := newTestDatasource(t, time.Minute)

	if response := ds.subscribe(context.Background(), subscribeQuery("runstream.subject", "uuid-run", 20*time.Millisecond), nc); response.Error != nil {
		t.Fatal(response.Error)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- ds.RunStream(ctx, &backend.RunStreamRequest{Path: "uuid-run"}, nil)
	}()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("expected a clean exit, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunStream did not return after its context was cancelled")
	}

	waitFor(t, "the stream to be removed", func() bool { return ds.streamResponsesSoFar.Len() == 0 })
	waitFor(t, "the subscription to be closed", func() bool { return nc.NumSubscriptions() == 0 })
}

// The message handler must never block, even when nobody reads the stream. It used to block forever
// once the notification channel was full, which stalled the subscription and piled messages up.
func TestMessageHandlerNeverBlocks(t *testing.T) {
	nc := startNatsServer(t)
	ds := newTestDatasource(t, time.Minute)

	if response := ds.subscribe(context.Background(), subscribeQuery("flood.subject", "uuid-flood", 20*time.Millisecond), nc); response.Error != nil {
		t.Fatal(response.Error)
	}
	entry := ds.streamResponsesSoFar.Get("uuid-flood")
	if entry == nil {
		t.Fatal("expected the stream to be cached")
	}

	// far more messages than the notification channel can hold, with nobody draining it.
	for i := 0; i < 500; i++ {
		if err := nc.Publish("flood.subject", []byte("{}")); err != nil {
			t.Fatal(err)
		}
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	// if the handler were blocked, the subscription would stop consuming and keep the messages
	// pending instead of delivering them.
	subscription := entry.Value().subscription
	waitFor(t, "all messages to be consumed", func() bool {
		pending, _, err := subscription.Pending()
		return err == nil && pending == 0
	})
	delivered, err := subscription.Delivered()
	if err != nil {
		t.Fatal(err)
	}
	if delivered < 500 {
		t.Errorf("expected at least 500 delivered messages, got %d", delivered)
	}
}

// Dispose must release every stream, subscription and the connection.
func TestDisposeReleasesStreams(t *testing.T) {
	nc := startNatsServer(t)
	instance, err := NewDatasource(context.Background(), backend.DataSourceInstanceSettings{UID: "test"})
	if err != nil {
		t.Fatal(err)
	}
	ds := instance.(*Datasource)
	ds.streamTTL = time.Minute

	for i := 0; i < 5; i++ {
		if response := ds.subscribe(context.Background(), subscribeQuery("dispose.subject", fmt.Sprintf("uuid-d%d", i), 20*time.Millisecond), nc); response.Error != nil {
			t.Fatal(response.Error)
		}
	}
	if ds.streamResponsesSoFar.Len() != 5 {
		t.Fatalf("expected 5 cached streams, got %d", ds.streamResponsesSoFar.Len())
	}

	ds.Dispose()

	if ds.streamResponsesSoFar.Len() != 0 {
		t.Errorf("expected no cached streams after Dispose, got %d", ds.streamResponsesSoFar.Len())
	}
	waitFor(t, "all subscriptions to be closed", func() bool { return nc.NumSubscriptions() == 0 })
}

// A script that subscribes must not leave the subscription on the shared connection.
func TestScriptSubscriptionsAreClosed(t *testing.T) {
	nc := startNatsServer(t)

	for i := 0; i < 10; i++ {
		if _, err := goja.RunScript(nc, `
			const inbox = nc.NewInbox();
			const subscription = nc.SubscribeSync(inbox);
			nc.PublishRequest("script.ping", inbox, "");
			return {received: String(subscription.NextMsg("10ms"))};
		`, 2*time.Second); err != nil {
			t.Fatal(err)
		}
	}

	waitFor(t, "the script subscriptions to be closed", func() bool { return nc.NumSubscriptions() == 0 })
}

// The same applies to a message mapping script.
func TestMappingScriptSubscriptionsAreClosed(t *testing.T) {
	nc := startNatsServer(t)

	for i := 0; i < 10; i++ {
		msg := &nats.Msg{Subject: "mapping.test", Data: []byte(`{"a": 1}`)}
		if _, err := goja.ConvertMessage(nc, msg, `
			const subscription = nc.SubscribeSync(nc.NewInbox());
			return {value: JSON.parse(msg.Data).a};
		`); err != nil {
			t.Fatal(err)
		}
	}

	waitFor(t, "the mapping subscriptions to be closed", func() bool { return nc.NumSubscriptions() == 0 })
}

// A message that arrives after the query gave up waiting must still reach the panel through the
// stream, and must not be delivered twice.
func TestFirstMessageAfterTimeoutIsStreamed(t *testing.T) {
	nc := startNatsServer(t)
	ds := newTestDatasource(t, time.Minute)

	response := ds.subscribe(context.Background(), subscribeQuery("late.subject", "uuid-late", 20*time.Millisecond), nc)
	if response.Error != nil {
		t.Fatal(response.Error)
	}
	// the query gave up waiting, so it returned an empty frame.
	if len(response.Frames[0].Fields) != 0 {
		t.Fatalf("expected an empty frame, got %d fields", len(response.Frames[0].Fields))
	}

	if err := nc.Publish("late.subject", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	entry := ds.streamResponsesSoFar.Get("uuid-late")
	if entry == nil {
		t.Fatal("expected the stream to still be cached")
	}
	waitFor(t, "the late message to reach the stream", func() bool {
		frame, _ := entry.Value().state()
		return frame != nil
	})
}
