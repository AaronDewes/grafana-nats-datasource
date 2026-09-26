package goja

import (
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/nats-io/nats.go"
)

// binaryPayload is not valid UTF-8: 0xff is never valid, and 0xc3 0x28 is an invalid sequence.
var binaryPayload = []byte{0x00, 0xff, 0xc3, 0x28, 0x41}

func convertMessage(t *testing.T, msg *nats.Msg, jsFn string) map[string]interface{} {
	t.Helper()
	frame, err := ConvertMessage(nil, msg, jsFn)
	if err != nil {
		t.Fatal(err)
	}
	row := map[string]interface{}{}
	for _, field := range frame.Fields {
		value, _ := field.ConcreteAt(0)
		row[field.Name] = value
	}
	return row
}

func assertEqualString(t *testing.T, expected string, actual interface{}) {
	t.Helper()
	got, ok := actual.(string)
	if !ok {
		t.Fatalf("expected a string, got %T", actual)
	}
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func assertEqualJSON(t *testing.T, expected string, actual interface{}) {
	t.Helper()
	actualJSON, err := json.Marshal(actual)
	if err != nil {
		t.Fatal(err)
	}
	if string(actualJSON) != expected {
		t.Errorf("expected %s, got %s", expected, actualJSON)
	}
}

func TestMsgRawDataKeepsBinaryPayload(t *testing.T) {
	msg := &nats.Msg{Subject: "test", Data: binaryPayload}
	row := convertMessage(t, msg, `return {bytes: Array.from(msg.RawData).join(","), length: msg.RawData.length};`)

	assertEqualString(t, "0,255,195,40,65", row["bytes"])
	assertEqualJSON(t, `5`, row["length"])
}

// msg.Data stays UTF-8 decoded, which is lossy for binary data - this documents that difference.
func TestMsgDataIsLossyForBinaryPayload(t *testing.T) {
	msg := &nats.Msg{Subject: "test", Data: binaryPayload}
	row := convertMessage(t, msg, `return {codes: Array.from(msg.Data).map(c => c.charCodeAt(0)).join(",")};`)

	assertEqualString(t, "0,65533,65533,40,65", row["codes"])
}

func TestRawDataIsACopy(t *testing.T) {
	msg := &nats.Msg{Subject: "test", Data: []byte{1, 2, 3}}
	row := convertMessage(t, msg, `
		const raw = msg.RawData;
		raw[0] = 99;
		return {modified: raw[0], original: msg.RawData[0]};
	`)

	assertEqualJSON(t, `99`, row["modified"])
	assertEqualJSON(t, `1`, row["original"])
}

func TestSettingBinaryData(t *testing.T) {
	for _, tc := range []struct {
		name     string
		js       string
		expected []byte
	}{
		{"Uint8Array", `msg.Data = new Uint8Array([0, 255, 195]);`, []byte{0, 255, 195}},
		{"ArrayBuffer", `msg.Data = new Uint8Array([7, 8]).buffer;`, []byte{7, 8}},
		{"subarray keeps the view bounds", `msg.Data = new Uint8Array([9, 1, 2]).subarray(1);`, []byte{1, 2}},
		{"RawData setter", `msg.RawData = new Uint8Array([255]);`, []byte{255}},
		{"string is UTF-8 encoded", `msg.Data = "hä";`, []byte{0x68, 0xc3, 0xa4}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := &nats.Msg{Subject: "test", Data: []byte("initial")}
			if _, err := ConvertMessage(nil, msg, tc.js+` return {ok: 1};`); err != nil {
				t.Fatal(err)
			}
			if string(msg.Data) != string(tc.expected) {
				t.Errorf("expected % x, got % x", tc.expected, msg.Data)
			}
		})
	}
}

func TestMsgpackDecodeScalars(t *testing.T) {
	msg := &nats.Msg{Subject: "test"}
	row := convertMessage(t, msg, `
		const d = (hex) => msgpack.decode(new Uint8Array(hex.split(" ").map(h => parseInt(h, 16))));
		const bin = d("c4 02 00 ff");
		return {result: [
			"posfixint=" + d("2a"),
			"negfixint=" + d("ff"),
			"uint32=" + d("ce ff ff ff ff"),
			// JS numbers are float64, so a large int64 loses precision - this documents that limit.
			"int64min=" + d("d3 80 00 00 00 00 00 00 00"),
			"int8=" + d("d0 ff"),
			"float64=" + d("cb 3f e0 00 00 00 00 00 00"),
			"float32=" + d("ca 3f 80 00 00"),
			"nil=" + d("c0"),
			"false=" + d("c2"),
			"true=" + d("c3"),
			"str=" + d("a3 61 62 63"),
			"bin=" + bin.constructor.name + "(" + Array.from(bin).join(",") + ")",
			"emptyarray=" + JSON.stringify(d("90")),
			"emptymap=" + JSON.stringify(d("80")),
			"nested=" + JSON.stringify(d("81 a1 61 91 01"))
		].join(" ")};
	`)

	assertEqualString(t, "posfixint=42 negfixint=-1 uint32=4294967295 int64min=-9223372036854776000 "+
		"int8=-1 float64=0.5 float32=1 nil=null false=false true=true str=abc bin=Uint8Array(0,255) "+
		`emptyarray=[] emptymap={} nested={"a":[1]}`, row["result"])
}

// The payloads below are what .NET MessagePack serializers produce for records: a map with the
// property names as keys, enums as their integer value, a DateTimeOffset as
// [timestamp, offsetInMinutes] and a TimeSpan as a count of 100ns ticks.
func TestMsgpackDecodeDotnetRecords(t *testing.T) {
	for _, tc := range []struct {
		name     string
		hex      string
		js       string
		expected string
	}{
		{
			name:     "record with an enum serialized as its integer value",
			hex:      "84A44E616D65A87769646765742D61A5436F756E7407A54C6162656CA24F4BA44B696E6402",
			js:       `JSON.stringify(v)`,
			expected: `{"Name":"widget-a","Count":7,"Label":"OK","Kind":2}`,
		},
		{
			name: "DateTimeOffset, null and TimeSpan",
			hex:  "83A953746172746564417492D6FF6AB797A000AA46696E69736865644174C0A8496E74657276616CCE23C34600",
			js: `v.StartedAt[0].toISOString() + " offset=" + v.StartedAt[1] +
				" finished=" + v.FinishedAt + " intervalSeconds=" + (v.Interval / 10000000)`,
			expected: `2026-09-26T10:00:00.000Z offset=0 finished=null intervalSeconds=60`,
		},
		{
			name:     "array of records",
			hex:      "9282A4486F7374A831302E302E312E31A547726F7570A377656282A4486F7374A831302E302E322E31A547726F7570A26462",
			js:       `v.length + ":" + v.map((e) => e.Host + "/" + e.Group).join(" ")`,
			expected: `2:10.0.1.1/web 10.0.2.1/db`,
		},
		{
			name: "nested record",
			hex:  "82A44974656D83A4496E666F84A44E616D65A87769646765742D61A45461677391A466617374A853657474696E677381A14BA156A5526174696F81A556616C7565CB3FE0000000000000A54F776E6572A7736F6D656F6E65A756657273696F6E03A84C6F636174696F6E82A4486F7374A831302E302E312E31A547726F7570A3776562",
			js: `v.Item.Info.Name + " v" + v.Item.Version + " by " + v.Item.Owner +
				" tags=" + v.Item.Info.Tags.join(",") + " K=" + v.Item.Info.Settings.K +
				" ratio=" + v.Item.Info.Ratio.Value + " -> " + v.Location.Host`,
			expected: `widget-a v3 by someone tags=fast K=V ratio=0.5 -> 10.0.1.1`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := hex.DecodeString(tc.hex)
			if err != nil {
				t.Fatal(err)
			}
			msg := &nats.Msg{Subject: "test", Data: raw}
			row := convertMessage(t, msg, `const v = msgpack.decode(msg.RawData); return {result: `+tc.js+`};`)
			assertEqualString(t, tc.expected, row["result"])
		})
	}
}

// A payload can also be a bare MessagePack integer instead of a map.
func TestMsgpackDecodeBareLong(t *testing.T) {
	msg := &nats.Msg{Subject: "test", Data: []byte{0x2a}}
	row := convertMessage(t, msg, `return {tick: msgpack.decode(msg.RawData)};`)
	assertEqualJSON(t, `42`, row["tick"])
}

func TestMsgpackDecodeTimestampToTimeField(t *testing.T) {
	// fixext4 timestamp -1: 2026-09-26T10:00:00Z
	raw, _ := hex.DecodeString("D6FF6AB797A0")
	msg := &nats.Msg{Subject: "test", Data: raw}
	frame, err := ConvertMessage(nil, msg, `return {time: msgpack.decode(msg.RawData)};`)
	if err != nil {
		t.Fatal(err)
	}
	field := frame.Fields[0]
	// a JS Date must arrive as a Grafana time field, so that it can be used as a time axis.
	if field.Type() != data.FieldTypeNullableTime {
		t.Fatalf("expected a nullable time field, got %s", field.Type().ItemTypeString())
	}
	value, _ := field.ConcreteAt(0)
	if got := value.(time.Time).UTC().Format(time.RFC3339); got != "2026-09-26T10:00:00Z" {
		t.Errorf("expected 2026-09-26T10:00:00Z, got %s", got)
	}
}

func TestMsgpackDecodeErrors(t *testing.T) {
	for _, tc := range []struct{ name, js string }{
		{"truncated map", `msgpack.decode(new Uint8Array([0x83, 0xa4]))`},
		{"trailing data", `msgpack.decode(new Uint8Array([0x2a, 0x2a]))`},
		{"unsupported type", `msgpack.decode(new Uint8Array([0xc1]))`},
		{"oversized length", `msgpack.decode(new Uint8Array([0xdb, 0xff, 0xff, 0xff, 0xff]))`},
		{"no data", `msgpack.decode(null)`},
		{"wrong argument type", `msgpack.decode(42)`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := RunScript(nil, `return {v: `+tc.js+`};`, time.Second); err == nil {
				t.Error("expected an error")
			}
		})
	}
}

// msgpack.decode also accepts a string, for payloads that happen to be valid UTF-8.
func TestMsgpackDecodeAcceptsString(t *testing.T) {
	msg := &nats.Msg{Subject: "test", Data: []byte{0xa3, 0x61, 0x62, 0x63}}
	row := convertMessage(t, msg, `return {value: msgpack.decode(msg.Data)};`)
	assertEqualString(t, "abc", row["value"])
}
