# NATS DataSource

NATS is an Open Source Message Bus on steroids, with many additional features over the core functionality.

If you use NATS as central system to connect different applications together, observability into the system is
crucial.

Features:

- **Request/Reply:** send a request on a certain topic, and visualize the response.
   - The response can be post-processed if needed via JavaScript.
   - This is useful if you want to *query* some connected system via NATS when the dashboard is opened.
- **Subscribe:** Listen to a certain topic, and visualize the messages as they stream into the system.
   - The messages can be post-processed if needed via JavaScript.
   - This is useful if you have a stream of continuous data (f.e. Logs) and you want to use them as they arrive.
- **Key/Value:** Read the entries of a JetStream Key/Value bucket, optionally including their history.
   - The entries can be post-processed if needed via JavaScript.
   - This is useful to show configuration or state stored in KV, or to chart a key's value over time.
- **Free-Form Script:** This is an advanced mode, which can send **multiple NATS requests**, wait for **multiple responses**
  and do **any kind of processing**. See below for examples.
- A default **Dashboard** which shows NATS system metrics via the `$SYS` account.

## Screenshots

![Query Editor](https://raw.githubusercontent.com/sandstorm/grafana-nats-datasource/main/docs/query-editor.png)

![Metrics Dashboard](https://raw.githubusercontent.com/sandstorm/grafana-nats-datasource/main/docs/metrics-dashboard.png)


## Request / Reply Mode explained

[NATS Request/Reply](https://docs.nats.io/nats-concepts/core-nats/reqreply) sends a request on the given subject with
an empty payload, and *renders the single response* (delivered to the _INBOX).

JSON messages can be rendered directly - nested JSON is flattened. Example messages:

```
{"key1": "val1", "key2": "value2"}

[{"key1": "val1", "key2": "value2"}, {"key1": "val3"}]
```


You can post-process each message via JavaScript, for example:

**Simple script**

```js
// This script is by default used in the backend if no script is given.

// msg.Data contains the received NATS message as string.
// by default, the last line of a script is returned automatically.
JSON.parse(msg.Data)
```

**Accessing Headers**

```js
// You can covert NATS message headers to columns (and in the same way, do any kind of calculation)

row = JSON.parse(msg.Data)
row["otherHeader"] = msg.Header.Get("My-Header")    

return row
```


### Scripting API

Input: `msg` contains the received message as a [nats.Msg](https://pkg.go.dev/github.com/nats-io/nats.go#Msg).

Supported Return values: A map `{k: "v"}`, a list of maps `[{k: "v"}]`,
a [data.Frame](https://pkg.go.dev/github.com/grafana/grafana-plugin-sdk-go@v0.147.0/data#Frame).



## Subscribe Mode explained

[NATS Subscribe](https://docs.nats.io/nats-concepts/core-nats/pubsub) listens to messages on the given subject pattern,
and sends them via [Grafana Live](https://grafana.com/docs/grafana/latest/setup-grafana/set-up-grafana-live/) to the
frontend.

JSON messages can be rendered directly - nested JSON is flattened. Example messages:

```
{"key1": "val1", "key2": "value2"}
```

You can post-process each message via JavaScript, for example:

**Simple script**

```js
// This script is by default used in the backend if no script is given.

// msg.Data contains the received NATS message as string.
// by default, the last line of a script is returned automatically.
JSON.parse(msg.Data)
```

**Accessing Headers**

```js
// You can covert NATS message headers to columns (and in the same way, do any kind of calculation)

row = JSON.parse(msg.Data)
row["otherHeader"] = msg.Header.Get("My-Header")    

return row
```

### Scripting API

Input: `msg` contains the received message as a [nats.Msg](https://pkg.go.dev/github.com/nats-io/nats.go#Msg).

Supported Return values: A map `{k: "v"}` (because the results are *streamed* to the UI).



## Binary payloads and MessagePack

`msg.Data` is the payload decoded as UTF-8. That is convenient for JSON and text, but **lossy for
binary payloads**, because invalid UTF-8 bytes are replaced by U+FFFD. For binary data use:

- `msg.RawData` - a `Uint8Array` with the exact bytes. Writing to `msg.Data` or `msg.RawData` accepts
  a string (UTF-8 encoded), an `ArrayBuffer`, a typed array or a `DataView`.
- `entry.rawValue` - the same for a Key/Value entry.
- `msgpack.decode(bytes)` - decodes a [MessagePack](https://msgpack.org/) payload, a common format
  for NATS clients (f.e. .NET ones). It takes a `Uint8Array`, an `ArrayBuffer` or a string.

```js
// read a MessagePack payload instead of JSON
const value = msgpack.decode(msg.RawData);
return {name: value.Name, count: value.Count};
```

`msgpack.decode` maps MessagePack to JS as follows:

| MessagePack | JavaScript |
| --- | --- |
| nil / bool / int / float / str | `null`, boolean, number, string |
| bin | `Uint8Array` |
| array / map | `Array` / object (non-string keys become strings) |
| timestamp (ext -1) | `Date`, which Grafana renders as a time field |
| other extensions | `{type, data}` with `data` as a `Uint8Array` |

Note that JS numbers are float64, so integers beyond 2^53 lose precision.

## Key/Value Mode explained

Reads the entries of a [JetStream Key/Value](https://docs.nats.io/nats-concepts/jetstream/key-value-store) bucket -
one row per key, with the columns `key`, `value`, `revision`, `created` and `operation`. Deleted keys are skipped.

- **Bucket:** the name of the bucket.
- **Key:** the key to read. Wildcards are allowed (f.e. `sensors.>`); leave empty for all keys.
- **Include history:** return all stored revisions (oldest first) instead of only the latest value.

You can post-process each entry via JavaScript, for example:

**JSON values**

```js
// entry contains key, value (as string), revision, created and operation.
// Here, the JSON value is expanded into columns, next to the key and creation time.
return Object.assign({key: entry.key, created: entry.created}, JSON.parse(entry.value));
```

**Numeric values** (f.e. to draw a time series together with *Include history*)

```js
return {created: entry.created, [entry.key]: Number(entry.value)};
```

For live updates, use the *Subscribe* mode on `$KV.<bucket>.>` - every change of a key is published on that subject.
Deletes arrive with an empty payload and the header `KV-Operation: DEL`.

### Scripting API

Input: `entry` contains the KV entry as `{key, value, revision, created, operation}`, where `value` is a string.

Supported Return values: A map `{k: "v"}`.



## Free-Form Script (advanced) explained

For advanced use cases, a free-form script can be used, which directly controls how messages
are sent and how their responses are processed:

- to *handle multiple replies to the same request*
- to send multiple *dependent requests*
- to *collect/reduce multiple responses* into a single UI response,
- other advanced cases.

The free-form script can return results directly or *stream them* to the UI. See the inline
script examples, they are heavily commented.

The API is basically like the Go API, but with errors transparently handled.

**Multiple Requests**

```js
// do two requests on different NATS subjects (json1 and json2)
const msg1 = nc.Request("json1", "", "50ms");
const msg2 = nc.Request("json2", "", "50ms");

// parse the response data as JSON
const parsed1 = JSON.parse(msg1.Data);
const parsed2 = JSON.parse(msg2.Data);

// return the concatenated list
return [parsed1, parsed2];
```

**Multiple Responses**

```js
// Sometimes, you receive *multiple responses* for a single request, f.e. when
// triggering $SYS.REQ.SERVER.PING in the SYS account, you will receive one answer
// per server.
//
// That's why we manually create an inbox for the reply; and poll it as
// long as there are messages.
const result = [];

const inbox = nc.NewInbox();
// The ordering is crucial: we first need to create the subscription, before
// sending the request (otherwise we might miss the response).
const subscription = nc.SubscribeSync(inbox);
nc.PublishRequest("$SYS.REQ.SERVER.PING", inbox, "");
while(true) {
  // we poll until we do not receive a message anymore within the given timeout.
  const msg = subscription.NextMsg("50ms");
  if (!msg) {
    // ... when this happens, we return the accumulated result.
    return result;
  }
  // here, we parse the given message.
  const parsed = JSON.parse(msg.Data);
  delete parsed.statsz.routes;
  result.push(parsed);
}
```

**Key/Value bucket**

```js
const bucket = kv("config");
const result = [];
for (const key of bucket.Keys("")) {
    const entry = bucket.Get(key);
    result.push({key: key, value: entry.value, revision: entry.revision});
}
return result;
```

### Scripting API

Input: `nc` the [nats.Conn](https://pkg.go.dev/github.com/nats-io/nats.go#Conn) you can use to:

- [nc.Subscribe()](https://pkg.go.dev/github.com/nats-io/nats.go#Conn.Subscribe) for subscribing to a topic;
- [nc.Request()](https://pkg.go.dev/github.com/nats-io/nats.go#Conn.Request) for sending out a request, and listening
  to a response
- any other interaction with the Go API.

`msgpack.decode(bytes)` decodes a MessagePack payload - see
[Binary payloads and MessagePack](#binary-payloads-and-messagepack).

`kv("bucket")` gives access to a JetStream Key/Value bucket:

- `Get(key)` returns the entry `{key, value, revision, created, operation}`, or `null` if the key does not exist;
- `Keys(filter)` returns the keys matching the filter (f.e. `"sensors.>"`, `""` for all keys);
- `Entries(filter)` returns the latest entry of all matching keys;
- `History(filter)` returns all revisions of all matching keys.

Each KV call is bounded by the *Request Timeout*.

Supported Return values: A map `{k: "v"}`, a list of maps `[{k: "v"}]`,
a [data.Frame](https://pkg.go.dev/github.com/grafana/grafana-plugin-sdk-go@v0.147.0/data#Frame).

## Developing

```
./dev.sh setup
./dev.sh build-backend
./dev.sh watch-frontend
./dev.sh up
# http://127.0.0.1:3000/plugins?filterBy=all&q=nats

# to hot-reload the plugin:
./dev.sh reload-plugin

./dev.sh test-nats-server
```

