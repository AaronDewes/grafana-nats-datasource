package goja

import (
	"context"
	"fmt"
	"github.com/dop251/goja"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/nats-io/nats.go"
	"github.com/sandstormmedia/nats/pkg/plugin/framestruct"
	"reflect"
	"sync"
	"time"
)

// gojaPool contains a pool of pre-initialized goja Javascript Engines:
// They have a _setup function defined (see setupFn), which wraps the NATS api and smoothens
// incompatibilities between JS and Golang:
//
//   - Msg.Data in NATS is a []byte. In JS, msg.Data is the UTF-8 decoded string (lossy for binary data),
//     and msg.RawData is a Uint8Array with the exact bytes. Data sent to NATS can be a string
//     (UTF-8 encoded), an ArrayBuffer, or any typed array / DataView.
//   - Timeouts are accepted as strings and auto-converted.
//   - msgpack.decode() decodes MessagePack payloads, which cannot be read via msg.Data.
//
// JS variables starting with "_" are internal, and are immutable. JS Variables
// starting with "__" are request-scoped.
var gojaPool = sync.Pool{
	New: func() any {
		vm := goja.New()
		vm.Set("log", func(msg string) {
			log.DefaultLogger.Info(msg)
		})
		vm.Set("_bytesToStr", func(bytes []byte) string {
			return string(bytes)
		})
		vm.Set("_strToBytes", func(in string) []byte {
			return []byte(in)
		})
		vm.Set("_bytesToBuffer", func(in []byte) goja.ArrayBuffer {
			// copy, so that modifications in JS do not change the original message.
			return vm.NewArrayBuffer(append([]byte{}, in...))
		})
		vm.Set("_bufferToBytes", func(in goja.ArrayBuffer) []byte {
			return append([]byte{}, in.Bytes()...)
		})
		vm.Set("_parseDuration", func(in string) (time.Duration, error) {
			return time.ParseDuration(in)
		})
		vm.Set("msgpack", newMsgpackObject(vm))
		vm.Set("_nats", &natsW{
			NewInbox: nats.NewInbox,
			Context:  nats.Context,
			NewMsg:   nats.NewMsg,
		})

		vm.RunString(setupFn)
		return vm
	},
}

// natsW is a struct wrapper for static functions of the nats.* package, to be able
// to pass them to JavaScript as "nats" (because we cannot pass a *package* by reference).
type natsW struct {
	NewInbox func() string
	Context  func(ctx context.Context) nats.ContextOpt
	NewMsg   func(subject string) *nats.Msg
}

// setupFn is the JS setup function registered during construction of Goja. See gojaPool for details.
const setupFn = `
"use strict";

// converts data to be sent to NATS to []byte: strings are UTF-8 encoded,
// ArrayBuffers, typed arrays and DataViews are used as-is.
function _toBytes(data) {
    if (data instanceof ArrayBuffer) {
        return _bufferToBytes(data);
    }
    if (ArrayBuffer.isView(data)) {
        return _bufferToBytes(new Uint8Array(data.buffer, data.byteOffset, data.byteLength).slice().buffer);
    }
    if (data === undefined || data === null) {
        return _strToBytes("");
    }
    return _strToBytes(String(data));
}

// converts a KV entry map from Go into a JS object, with rawValue being a Uint8Array.
// rawValue is not enumerable, so that it is not rendered if the entry is returned as a row.
function _wrapKVEntry(__entry, __raw) {
    if (!__entry) {
        // keep falsy objects
        return __entry;
    }
    const entry = {};
    for (const k of ["key", "value", "revision", "created", "operation"]) {
        entry[k] = __entry[k];
    }
    Object.defineProperty(entry, "rawValue", {
        get() {
            return new Uint8Array(_bytesToBuffer(__raw === undefined ? __entry.rawValue : __raw));
        }
    });
    return entry;
}

function _wrapKVBucket(__bucket) {
    const bucket = Object.create(__bucket);
    bucket.Get = (key) => _wrapKVEntry(__bucket.Get(key));
    bucket.Entries = (filter) => Array.from(__bucket.Entries(filter), (e) => _wrapKVEntry(e));
    bucket.History = (filter) => Array.from(__bucket.History(filter), (e) => _wrapKVEntry(e));
    bucket.Keys = (filter) => Array.from(__bucket.Keys(filter));
    return bucket;
}

function _setup(_nats, _bytesToStr, _strToBytes, _parseDuration) {
    function wrapNc(__nc) {
        const nc = Object.create(__nc);
        
        nc.Publish = (subj, data) => __nc.Publish(subj, _toBytes(data));
        nc.PublishRequest = (subj, reply, data) => __nc.PublishRequest(subj, reply, _toBytes(data));
        nc.QueueSubscribe = (subj, queue, cb) => wrapSubscription(__nc.QueueSubscribe(subj, queue, (__msg) => cb(wrapMsg(__msg))));
        nc.QueueSubscribeSync = (subj, queue) => wrapSubscription(__nc.QueueSubscribeSync(subj, queue));
        nc.Request = (subj, data, timeout) => wrapMsg(__nc.Request(subj, _toBytes(data), _parseDuration(timeout)));
        nc.RequestMsg = (msg, timeout) => wrapMsg(__nc.RequestMsg(msg, _parseDuration(timeout)));
        nc.RequestWithContext = (ctx, subj, data) => wrapMsg(__nc.RequestWithContext(ctx, subj, _toBytes(data)));
        nc.Subscribe = (subj, cb) => wrapSubscription(__nc.Subscribe(subj, (__msg) => cb(wrapMsg(__msg))));
        nc.SubscribeSync = (subj) => wrapSubscription(__nc.SubscribeSync(subj));
		return nc;
    }
    
	function wrapMsg(__msg) {
        if (!__msg) {
            // keep falsy objects
            return __msg;
        }
		const msg = Object.create(__msg);
		Object.defineProperty(msg, "Data", {
			get() {
				return _bytesToStr(__msg.Data);
			},
			set(value) {
                __msg.Data = _toBytes(value);
			}
		});
		Object.defineProperty(msg, "RawData", {
			get() {
				return new Uint8Array(_bytesToBuffer(__msg.Data));
			},
			set(value) {
                __msg.Data = _toBytes(value);
			}
		});
		return msg;
	}

    function wrapNats(__nats) {
		const nats = Object.create(__nats);
        
        nats.NewMsg = (subject) => wrapMsg(__nats.NewMsg(subject));
        
        return nats;
	}
    
    function nullOnTimeout(func) {
        return function() {
            try {
            	return func.apply(this, arguments);
			} catch (e) {
                return null;
			}
        }
	}
    
    function wrapSubscription(__subscription) {
        if (!__subscription) {
            // keep falsy objects
            return __subscription;
        }
        // Register the subscription, so that it is ended when the script is done. typeof is safe
        // here even in strict mode, because the tracker is only defined while a script runs.
        if (typeof __trackSubscription === "function") {
            __trackSubscription(__subscription);
        }
        
        const subscription = Object.create(__subscription);
        subscription.NextMsg = nullOnTimeout((timeout) => wrapMsg(__subscription.NextMsg(_parseDuration(timeout))));
        subscription.NextMsgWithContext = (ctx) => wrapMsg(__subscription.NextMsgWithContext(ctx));
        
        return subscription;
    } 
    
    return function(__nc, __msg = null) {
		const nats = wrapNats(_nats);
        const nc = wrapNc(__nc);
		const msg = wrapMsg(__msg);
		return {
            nats,
            nc,
            msg
		};  
    }
    
    
}
`

// wrapJs wraps the user-defined script.
func wrapJs(in string) string {
	return fmt.Sprintf(`
	"use strict";
	(function() {
		const {nats, nc, msg} = _setup(_nats, _bytesToStr, _strToBytes, _parseDuration)(__nc, __msg);
		%s;
    })()
`, in)
}
func wrapJsScript(in string) string {
	return fmt.Sprintf(`
	"use strict";
	(function() {
		const {nats, nc} = _setup(_nats, _bytesToStr, _strToBytes, _parseDuration)(__nc, undefined);
		const kv = (bucket) => _wrapKVBucket(__kv(bucket));
		%s;
    })()
`, in)
}

// trackSubscriptions registers a tracker for subscriptions a script creates, and returns a cleanup
// function that ends them again. Without it, a script that subscribes - f.e. to collect several
// replies to one request - leaves the subscription on the shared connection for good, and every
// dashboard refresh adds another one.
func trackSubscriptions(vm *goja.Runtime) func() {
	var subscriptions []*nats.Subscription
	_ = vm.Set("__trackSubscription", func(subscription *nats.Subscription) {
		if subscription != nil {
			subscriptions = append(subscriptions, subscription)
		}
	})
	return func() {
		for _, subscription := range subscriptions {
			_ = subscription.Unsubscribe()
		}
		_ = vm.GlobalObject().Delete("__trackSubscription")
	}
}

func ConvertMessage(nc *nats.Conn, msg *nats.Msg, jsFn string) (*data.Frame, error) {
	if jsFn == "" {
		jsFn = `
			return JSON.parse(msg.Data);
		`
	}
	vm := gojaPool.Get().(*goja.Runtime)
	defer gojaPool.Put(vm)
	if err := vm.Set("__nc", nc); err != nil {
		return nil, err
	}
	if err := vm.Set("__msg", msg); err != nil {
		return nil, err
	}
	defer trackSubscriptions(vm)()
	// reset request-scoped variables - this way, we can have a clean VM again.
	defer func() {
		_ = vm.GlobalObject().Delete("__nc")
		_ = vm.GlobalObject().Delete("__msg")
	}()

	resultWrapper, err := vm.RunString(wrapJs(jsFn))
	if err != nil {
		return nil, fmt.Errorf("could not run JS: %w  - JS was: %s", err, wrapJs(jsFn))
	}

	result := resultWrapper.Export()
	return convertResult(result)
}

// RunScript runs a free-form script. timeout bounds each KV operation of the script.
func RunScript(nc *nats.Conn, jsFn string, timeout time.Duration) (*data.Frame, error) {
	if jsFn == "" {
		return nil, fmt.Errorf("script must be specified")
	}
	vm := gojaPool.Get().(*goja.Runtime)
	defer gojaPool.Put(vm)
	if err := vm.Set("__nc", nc); err != nil {
		return nil, err
	}
	if err := vm.Set("__kv", kvFn(nc, timeout)); err != nil {
		return nil, err
	}
	defer trackSubscriptions(vm)()
	// reset request-scoped variables - this way, we can have a clean VM again.
	defer func() {
		_ = vm.GlobalObject().Delete("__nc")
		_ = vm.GlobalObject().Delete("__kv")
	}()

	resultWrapper, err := vm.RunString(wrapJsScript(jsFn))
	if err != nil {
		return nil, fmt.Errorf("could not run JS: %w  - JS was: %s", err, wrapJs(jsFn))
	}

	result := resultWrapper.Export()
	return convertResult(result)
}

func convertResult(result interface{}) (*data.Frame, error) {
	_, isMap := result.(map[string]interface{})
	_, isArray := result.([]interface{})
	_, isFrame := result.(data.Frame)
	_, isFramePtr := result.(*data.Frame)

	if isFrame {
		frame := result.(data.Frame)
		return &frame, nil
	}
	if isFramePtr {
		frame := result.(*data.Frame)
		return frame, nil
	}
	if isMap {
		mapEl := result.(map[string]interface{})
		return framestruct.ToDataFrame("result", mapEl)
	}
	if isArray {
		arr := result.([]interface{})
		arrayOfMap := make([]map[string]interface{}, 0, len(arr))
		for i, v := range arr {
			conv, ok := v.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("result of script was []any, but not []map[string]any. Index %d was of type %s", i, reflect.TypeOf(v).String())
			}
			arrayOfMap = append(arrayOfMap, conv)
		}
		return framestruct.ToDataFrame("result", arrayOfMap)
	}

	return nil, fmt.Errorf("result of script must be map[string]interface{}, []map[string]interface{}, or data.Frame. Was: %v", reflect.TypeOf(result))
}
