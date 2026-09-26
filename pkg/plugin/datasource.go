package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana-plugin-sdk-go/live"
	"github.com/jellydator/ttlcache/v3"
	"github.com/nats-io/nats.go"
	"github.com/sandstormmedia/nats/pkg/plugin/goja"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
)

// Make sure Datasource implements required interfaces. This is important to do
// since otherwise we will only get a not implemented error response from plugin in
// runtime.
var (
	_ backend.QueryDataHandler   = (*Datasource)(nil)
	_ backend.CheckHealthHandler = (*Datasource)(nil)

	// TODO: https://grafana.com/tutorials/build-a-streaming-data-source-plugin/
	_ backend.StreamHandler         = (*Datasource)(nil)
	_ instancemgmt.InstanceDisposer = (*Datasource)(nil)
)

// streamResponseTTL is how long an unused streaming response is kept. The TTL is extended on every
// message, so it only expires for streams nobody is feeding or reading any more.
const streamResponseTTL = 5 * time.Minute

// NewDatasource creates a new datasource instance.
func NewDatasource(ctx context.Context, config backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	ds := &Datasource{
		uid:                  config.UID,
		streamTTL:            streamResponseTTL,
		streamResponsesSoFar: ttlcache.New[string, *streamResponse](),
	}
	// An evicted stream must end its NATS subscription, otherwise the subscription outlives the
	// entry that owns it and keeps receiving messages nobody reads.
	ds.streamResponsesSoFar.OnEviction(func(_ context.Context, _ ttlcache.EvictionReason, item *ttlcache.Item[string, *streamResponse]) {
		item.Value().close()
	})
	// ttlcache only removes expired items while this loop runs. Without it, Get returns nil for an
	// expired entry but the entry itself stays in the cache forever.
	go ds.streamResponsesSoFar.Start()
	return ds, nil
}

// Dispose here tells plugin SDK that plugin wants to clean up resources when a new instance
// created. As soon as datasource settings change detected by SDK old datasource instance will
// be disposed and a new one will be created using NewSampleDatasource factory function.
func (ds *Datasource) Dispose() {
	// Clean up datasource instance resources.
	ds.streamResponsesSoFar.Stop()
	// DeleteAll evicts every entry, which ends the NATS subscriptions via the eviction hook above.
	ds.streamResponsesSoFar.DeleteAll()
	ds.closeNats()
}

// Datasource is an example datasource which can respond to data queries, reports
// its health and has streaming skills.
type Datasource struct {
	uid                  string
	streamResponsesSoFar *ttlcache.Cache[string, *streamResponse]
	// streamTTL is how long an unused streaming response is kept; a field so that tests can shorten it.
	streamTTL time.Duration

	// natsConnMu guards natsConn, so that only one NATS connection is created without any race conditions
	natsConnMu sync.Mutex
	// natsConn contains the singleton NATS connection for the datasource. Never access this directly, but always use connectNats.
	natsConn *nats.Conn
}

// streamResponse holds the state of a single streaming query. The NATS message handler writes to it
// while RunStream reads from it, so the fields are guarded by mu.
type streamResponse struct {
	mu           sync.Mutex
	currentFrame *data.Frame
	currentErr   error
	subscription *nats.Subscription

	// onNewMessages wakes RunStream up. It only signals that something changed - the frame itself
	// is read from currentFrame - so a full buffer can be ignored instead of blocking the sender.
	onNewMessages chan bool

	cancel    context.CancelFunc
	closeOnce sync.Once
}

// notify stores a new frame or error and wakes RunStream, without ever blocking the caller. The
// NATS message handler runs on the subscription's dispatch goroutine, so blocking here would stall
// the subscription and pile messages up in its pending queue until they are dropped.
func (sr *streamResponse) notify(frame *data.Frame, err error) {
	sr.mu.Lock()
	if frame != nil {
		sr.currentFrame = frame
	}
	if err != nil {
		sr.currentErr = err
	}
	sr.mu.Unlock()

	select {
	case sr.onNewMessages <- true:
	default:
		// RunStream has not caught up yet - it reads the latest frame on its next wake-up anyway.
	}
}

// state returns the latest frame and error.
func (sr *streamResponse) state() (*data.Frame, error) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	return sr.currentFrame, sr.currentErr
}

func (sr *streamResponse) setSubscription(subscription *nats.Subscription) {
	sr.mu.Lock()
	sr.subscription = subscription
	sr.mu.Unlock()
}

// close ends the NATS subscription right away, instead of waiting for the next message to notice
// the cancelled context. It is safe to call several times and from several goroutines.
func (sr *streamResponse) close() {
	sr.closeOnce.Do(func() {
		if sr.cancel != nil {
			sr.cancel()
		}
		sr.mu.Lock()
		subscription := sr.subscription
		sr.subscription = nil
		sr.currentFrame = nil
		sr.mu.Unlock()

		if subscription != nil {
			_ = subscription.Unsubscribe()
		}
	})
}

func (ds *Datasource) SubscribeStream(_ context.Context, request *backend.SubscribeStreamRequest) (*backend.SubscribeStreamResponse, error) {
	status := backend.SubscribeStreamStatusNotFound

	value := ds.streamResponsesSoFar.Get(request.Path)
	if value != nil {
		// found the stream, so we can subscribe to it.
		status = backend.SubscribeStreamStatusOK
	}
	return &backend.SubscribeStreamResponse{
		Status: status,
	}, nil
}

func (ds *Datasource) PublishStream(_ context.Context, _ *backend.PublishStreamRequest) (*backend.PublishStreamResponse, error) {
	// we do not allow any write operation from the frontend (so far)
	return &backend.PublishStreamResponse{
		Status: backend.PublishStreamStatusPermissionDenied,
	}, nil
}

func (ds *Datasource) RunStream(ctx context.Context, request *backend.RunStreamRequest, sender *backend.StreamSender) error {
	value := ds.streamResponsesSoFar.Get(request.Path)
	if value == nil {
		return fmt.Errorf("no data found for stream %s", request.Path)
	}

	// The subscription exists to feed this stream, so it ends together with it. Dropping the cache
	// entry runs the eviction hook, which unsubscribes and releases the last frame.
	defer ds.streamResponsesSoFar.Delete(request.Path)

	sr := value.Value()
	for {
		select {
		case <-ctx.Done():
			// the panel is gone - we are done.
			return nil
		case <-sr.onNewMessages:
			frame, err := sr.state()
			if err != nil {
				// error while processing messages -> exit stream
				return err
			}
			if frame == nil {
				continue
			}
			if err := sender.SendFrame(frame, data.IncludeAll); err != nil {
				return err
			}
		}
	}
}

// QueryData handles multiple queries and returns multiple responses.
// req contains the queries []DataQuery (where each query contains RefID as a unique identifier).
// The QueryDataResponse contains a map of RefID to the response for each query, and each response
// contains Frames ([]*Frame).
func (ds *Datasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	// when logging at a non-Debug level, make sure you don't include sensitive information in the message
	// (like the *backend.QueryDataRequest)
	log.DefaultLogger.Debug("QueryData called", "numQueries", len(req.Queries))

	// create response struct
	response := backend.NewQueryDataResponse()

	// loop over queries and execute them individually.
	for _, q := range req.Queries {
		res := ds.query(ctx, req.PluginContext, q)

		// save the response in a hashmap
		// based on with RefID as identifier
		response.Responses[q.RefID] = res
	}

	return response, nil
}

func (ds *Datasource) loadDataSourceOptions(pCtx backend.PluginContext) (*MyDataSourceOptions, *MySecureJsonData, error) {
	var dataSourceOptions *MyDataSourceOptions
	err := json.Unmarshal(pCtx.DataSourceInstanceSettings.JSONData, &dataSourceOptions)
	if err != nil {
		return nil, nil, fmt.Errorf("data source options json unmarshal: %w", err)
	}
	var dataSourceSecureOptions *MySecureJsonData
	secureBytes, err := json.Marshal(pCtx.DataSourceInstanceSettings.DecryptedSecureJSONData)
	if err != nil {
		return nil, nil, fmt.Errorf("decrypted secureJson could not be converted to JSON: %w", err)
	}

	err = json.Unmarshal(secureBytes, &dataSourceSecureOptions)
	if err != nil {
		return nil, nil, fmt.Errorf("decrypted secureJson could not be parsed: %w", err)
	}
	return dataSourceOptions, dataSourceSecureOptions, nil
}

func (ds *Datasource) query(ctx context.Context, pCtx backend.PluginContext, query backend.DataQuery) backend.DataResponse {
	//////////////
	// 1) Data Source option loading
	//////////////
	dataSourceOptions, dataSourceSecureOptions, err := ds.loadDataSourceOptions(pCtx)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, "data source could not be loaded: "+err.Error())
	}

	//////////////
	// 2) Connect
	//////////////
	nc, err := ds.connectNats(dataSourceOptions, dataSourceSecureOptions)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, "NATS connection error:  "+err.Error())
	}
	// TODO: later, keep the nats connection open for some minutes instead of tearing it down for every req.
	//defer nc.Close()

	//////////////
	// 3) do request
	//////////////
	// Unmarshal the JSON into our queryModel.
	var qm queryModel

	err = json.Unmarshal(query.JSON, &qm)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, "json unmarshal: "+err.Error())
	}

	if qm.RequestTimeout.Duration == 0 {
		qm.RequestTimeout.Duration = 5 * time.Second
	}
	if qm.QueryType == QueryTypeRequestReply {
		frame, err := ds.requestReply(nc, qm)
		if err != nil {
			return backend.ErrDataResponse(backend.StatusBadRequest, "Response conversion error: "+err.Error())
		}

		return backend.DataResponse{
			Frames: data.Frames{
				frame,
			},
			Status: backend.StatusOK,
		}
	} else if qm.QueryType == QueryTypeSubscribe {
		return ds.subscribe(ctx, qm, nc)
	} else if qm.QueryType == QueryTypeScript {
		return ds.script(ctx, qm, nc)
	} else if qm.QueryType == QueryTypeKV {
		return ds.kv(ctx, qm, nc)
	} else {
		return backend.ErrDataResponse(backend.StatusBadRequest, "Invalid Query Type: "+qm.QueryType)
	}
}

// CheckHealth handles health checks sent from Grafana to the plugin.
// The main use case for these health checks is the test button on the
// datasource configuration page which allows users to verify that
// a datasource is working as expected.
func (ds *Datasource) CheckHealth(_ context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	// when logging at a non-Debug level, make sure you don't include sensitive information in the message
	// (like the *backend.QueryDataRequest)
	log.DefaultLogger.Debug("CheckHealth called")

	//////////////
	// 1) Data Source option loading
	//////////////
	dataSourceOptions, dataSourceSecureOptions, err := ds.loadDataSourceOptions(req.PluginContext)
	if err != nil {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: "Data source options could not be loaded (should never happen)" + err.Error(),
		}, nil
	}

	//////////////
	// 2) Connect
	//////////////
	_, err = ds.connectNats(dataSourceOptions, dataSourceSecureOptions)
	if err != nil {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: "NATS could not be connected to: " + err.Error(),
		}, nil
	}
	// NOTE: do not close the connection here - it is shared with all queries of this datasource.

	return &backend.CheckHealthResult{
		Status:  backend.HealthStatusOk,
		Message: "Data source is working",
	}, nil
}

func (ds *Datasource) requestReply(nc *nats.Conn, qm queryModel) (*data.Frame, error) {
	resp, err := nc.Request(qm.NatsSubject, []byte(qm.RequestData), qm.RequestTimeout.Duration)
	if err != nil {
		return nil, err
	}

	return goja.ConvertMessage(nc, resp, qm.JsFn)
}

// subscribe handles a NATS subscription call in streaming fashion.
// TODO explain how done
// inspired by https://github.com/grafana/grafana-iot-twinmaker-app/blob/0947ce1ff0afec8372cae624566726e68687137b/pkg/plugin/datasource.go
func (ds *Datasource) subscribe(ctx context.Context, qm queryModel, nc *nats.Conn) backend.DataResponse {
	requestUuid := uuid.NewString()
	if len(qm.StreamRequestUuidForTesting) > 0 {
		requestUuid = qm.StreamRequestUuidForTesting
	}

	// The subscription outlives this request - it is handed over to RunStream - so it gets its own
	// context instead of the request's, and is ended via sr.close().
	subscriptionCtx, cancel := context.WithCancel(context.Background())
	sr := &streamResponse{
		onNewMessages: make(chan bool, 1),
		cancel:        cancel,
	}

	// firstResult carries the first message back to this request, which answers it synchronously so
	// that the panel gets a frame with a schema. The channel is deliberately unbuffered: a send only
	// succeeds while this request is still waiting, so a message that arrives after the timeout is
	// streamed instead of being dropped, and never counted twice.
	type firstResult struct {
		frame *data.Frame
		err   error
	}
	first := make(chan firstResult)

	// handOver passes a result to the waiting request, and reports whether it got there.
	handOver := func(result firstResult) bool {
		select {
		case first <- result:
			return true
		default:
			return false
		}
	}

	messages := 0
	subscription, err := nc.Subscribe(qm.NatsSubject, func(msg *nats.Msg) {
		if subscriptionCtx.Err() != nil {
			// the stream is gone - make sure the subscription goes with it.
			sr.close()
			return
		}
		log.DefaultLogger.Debug("Received NATS Message")
		// extend the TTL every time we receive a message.
		ds.streamResponsesSoFar.Touch(requestUuid)
		messages++

		frame, err := goja.ConvertMessage(nc, msg, qm.JsFn)
		if err != nil {
			err = fmt.Errorf("could not convert message %d: %w", messages, err)
			log.DefaultLogger.Error(err.Error())
			if !handOver(firstResult{err: err}) {
				sr.notify(nil, err)
			}
			// a broken script will not fix itself on the next message, so stop here.
			sr.close()
			return
		}

		if handOver(firstResult{frame: frame}) {
			return
		}
		sr.notify(frame, nil)
	})
	if err != nil {
		cancel()
		return backend.ErrDataResponse(backend.StatusBadRequest, "could not create subscription: "+err.Error())
	}
	sr.setSubscription(subscription)
	ds.streamResponsesSoFar.Set(requestUuid, sr, ds.streamTTL)
	log.DefaultLogger.Debug(fmt.Sprintf("%s: Subscription set up for %s", requestUuid, qm.NatsSubject))

	// Grafana reaches the live channel via the frame's metadata.
	channel := live.Channel{
		Scope:     live.ScopeDatasource,
		Namespace: ds.uid,
		// because the request UUID is random, we cannot snoop on other people's values (security),
		// and we have one subscription per user (which is what we want in our case).
		Path: requestUuid,
	}
	meta := &data.FrameMeta{Channel: channel.String()}

	// Wait for the first message, but no longer than the request timeout: on a quiet subject this
	// request would otherwise block forever, leaking this goroutine and the subscription with it.
	timeout := time.NewTimer(qm.RequestTimeout.Duration)
	defer timeout.Stop()

	select {
	case result := <-first:
		if result.err != nil {
			ds.streamResponsesSoFar.Delete(requestUuid)
			return backend.ErrDataResponse(backend.StatusBadRequest, "error handling 1st message: "+result.err.Error())
		}
		result.frame.SetMeta(meta)
		return backend.DataResponse{
			Frames: data.Frames{result.frame},
			Status: backend.StatusOK,
		}

	case <-timeout.C:
		// No message so far. Hand Grafana an empty frame on the live channel, so that the panel
		// subscribes and fills up once messages start arriving. Anything that arrives from here on
		// reaches the panel through the stream instead.
		log.DefaultLogger.Debug(fmt.Sprintf("%s: no message on %s within %s, streaming anyway",
			requestUuid, qm.NatsSubject, qm.RequestTimeout.Duration))
		frame := data.NewFrame("response")
		frame.SetMeta(meta)
		return backend.DataResponse{
			Frames: data.Frames{frame},
			Status: backend.StatusOK,
		}

	case <-ctx.Done():
		// the query was cancelled, f.e. because the dashboard was closed while we were waiting.
		ds.streamResponsesSoFar.Delete(requestUuid)
		return backend.ErrDataResponse(backend.StatusBadRequest, "query cancelled: "+ctx.Err().Error())
	}
}

// script allows free-form scripts
// TODO explain how done
func (ds *Datasource) script(_ context.Context, qm queryModel, natsConn *nats.Conn) backend.DataResponse {
	frame, err := goja.RunScript(natsConn, qm.JsFn, qm.RequestTimeout.Duration)

	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, "error handling 1st message: "+err.Error())
	}

	return backend.DataResponse{
		Frames: data.Frames{
			frame,
		},
		Status: backend.StatusOK,
	}
}
