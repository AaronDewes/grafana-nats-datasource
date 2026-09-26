package goja

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/dop251/goja"
)

// msgpackDecoder decodes MessagePack into JS values. MessagePack is a common payload format on
// NATS (f.e. for .NET clients), and cannot be read via msg.Data, because it is not valid UTF-8.
//
// The values are mapped as follows:
//
//	nil                  -> null
//	bool                 -> boolean
//	int / uint           -> number (int64; uint64 above 2^63-1 becomes a float64)
//	float32 / float64    -> number
//	str                  -> string
//	bin                  -> Uint8Array
//	array                -> Array
//	map                  -> object (non-string keys are converted to strings)
//	ext -1 (timestamp)   -> Date
//	other ext            -> {type: number, data: Uint8Array}
type msgpackDecoder struct {
	vm   *goja.Runtime
	data []byte
	pos  int
}

// decodeMsgpack decodes the MessagePack document in data. Trailing bytes are an error, so that
// a wrong payload format does not silently decode to a partial value.
func decodeMsgpack(vm *goja.Runtime, data []byte) (goja.Value, error) {
	d := &msgpackDecoder{vm: vm, data: data}
	value, err := d.decodeValue(0)
	if err != nil {
		return nil, err
	}
	if d.pos != len(d.data) {
		return nil, fmt.Errorf("unexpected trailing data at offset %d (decoded %d of %d bytes)", d.pos, d.pos, len(d.data))
	}
	return value, nil
}

// maxMsgpackDepth bounds the nesting depth, so that a malformed payload cannot exhaust the stack.
const maxMsgpackDepth = 100

func (d *msgpackDecoder) decodeValue(depth int) (goja.Value, error) {
	if depth > maxMsgpackDepth {
		return nil, fmt.Errorf("nested too deeply (more than %d levels)", maxMsgpackDepth)
	}
	c, err := d.readByte()
	if err != nil {
		return nil, err
	}

	switch {
	case c <= 0x7f: // positive fixint
		return d.vm.ToValue(int64(c)), nil
	case c >= 0xe0: // negative fixint
		return d.vm.ToValue(int64(int8(c))), nil
	case c >= 0x80 && c <= 0x8f: // fixmap
		return d.decodeMap(int(c&0x0f), depth)
	case c >= 0x90 && c <= 0x9f: // fixarray
		return d.decodeArray(int(c&0x0f), depth)
	case c >= 0xa0 && c <= 0xbf: // fixstr
		return d.decodeString(int(c & 0x1f))
	}

	switch c {
	case 0xc0:
		return goja.Null(), nil
	case 0xc2:
		return d.vm.ToValue(false), nil
	case 0xc3:
		return d.vm.ToValue(true), nil
	case 0xc4, 0xc5, 0xc6: // bin 8 / 16 / 32
		length, err := d.readLength(1 << (c - 0xc4))
		if err != nil {
			return nil, err
		}
		return d.decodeBin(length)
	case 0xc7, 0xc8, 0xc9: // ext 8 / 16 / 32
		length, err := d.readLength(1 << (c - 0xc7))
		if err != nil {
			return nil, err
		}
		return d.decodeExt(length)
	case 0xca: // float 32
		bits, err := d.readUint(4)
		if err != nil {
			return nil, err
		}
		return d.vm.ToValue(float64(math.Float32frombits(uint32(bits)))), nil
	case 0xcb: // float 64
		bits, err := d.readUint(8)
		if err != nil {
			return nil, err
		}
		return d.vm.ToValue(math.Float64frombits(bits)), nil
	case 0xcc, 0xcd, 0xce, 0xcf: // uint 8 / 16 / 32 / 64
		value, err := d.readUint(1 << (c - 0xcc))
		if err != nil {
			return nil, err
		}
		if value > math.MaxInt64 {
			// does not fit into an int64 - JS numbers are floats anyway.
			return d.vm.ToValue(float64(value)), nil
		}
		return d.vm.ToValue(int64(value)), nil
	case 0xd0, 0xd1, 0xd2, 0xd3: // int 8 / 16 / 32 / 64
		size := 1 << (c - 0xd0)
		value, err := d.readUint(size)
		if err != nil {
			return nil, err
		}
		// sign-extend the read bytes.
		shift := 64 - 8*uint(size)
		return d.vm.ToValue(int64(value<<shift) >> shift), nil
	case 0xd4, 0xd5, 0xd6, 0xd7, 0xd8: // fixext 1 / 2 / 4 / 8 / 16
		return d.decodeExt(1 << (c - 0xd4))
	case 0xd9, 0xda, 0xdb: // str 8 / 16 / 32
		length, err := d.readLength(1 << (c - 0xd9))
		if err != nil {
			return nil, err
		}
		return d.decodeString(length)
	case 0xdc, 0xdd: // array 16 / 32
		length, err := d.readLength(2 << (c - 0xdc))
		if err != nil {
			return nil, err
		}
		return d.decodeArray(length, depth)
	case 0xde, 0xdf: // map 16 / 32
		length, err := d.readLength(2 << (c - 0xde))
		if err != nil {
			return nil, err
		}
		return d.decodeMap(length, depth)
	}

	return nil, fmt.Errorf("unsupported MessagePack type 0x%02x at offset %d", c, d.pos-1)
}

func (d *msgpackDecoder) decodeString(length int) (goja.Value, error) {
	b, err := d.readBytes(length)
	if err != nil {
		return nil, err
	}
	return d.vm.ToValue(string(b)), nil
}

func (d *msgpackDecoder) decodeBin(length int) (goja.Value, error) {
	b, err := d.readBytes(length)
	if err != nil {
		return nil, err
	}
	return d.newUint8Array(b)
}

func (d *msgpackDecoder) decodeArray(length int, depth int) (goja.Value, error) {
	values := make([]interface{}, length)
	for i := 0; i < length; i++ {
		value, err := d.decodeValue(depth + 1)
		if err != nil {
			return nil, err
		}
		values[i] = value
	}
	return d.vm.NewArray(values...), nil
}

func (d *msgpackDecoder) decodeMap(length int, depth int) (goja.Value, error) {
	object := d.vm.NewObject()
	for i := 0; i < length; i++ {
		key, err := d.decodeValue(depth + 1)
		if err != nil {
			return nil, err
		}
		value, err := d.decodeValue(depth + 1)
		if err != nil {
			return nil, err
		}
		if err := object.Set(key.String(), value); err != nil {
			return nil, err
		}
	}
	return object, nil
}

// decodeExt decodes an extension value of the given payload length. Timestamps (type -1) become
// a Date, everything else a {type, data} object.
func (d *msgpackDecoder) decodeExt(length int) (goja.Value, error) {
	extType, err := d.readByte()
	if err != nil {
		return nil, err
	}
	payload, err := d.readBytes(length)
	if err != nil {
		return nil, err
	}

	if int8(extType) == -1 {
		timestamp, err := decodeMsgpackTimestamp(payload)
		if err != nil {
			return nil, err
		}
		return d.newDate(timestamp)
	}

	data, err := d.newUint8Array(payload)
	if err != nil {
		return nil, err
	}
	object := d.vm.NewObject()
	if err := object.Set("type", int64(int8(extType))); err != nil {
		return nil, err
	}
	if err := object.Set("data", data); err != nil {
		return nil, err
	}
	return object, nil
}

// decodeMsgpackTimestamp decodes the timestamp extension (type -1) in its 32, 64 and 96 bit forms.
func decodeMsgpackTimestamp(payload []byte) (time.Time, error) {
	switch len(payload) {
	case 4:
		return time.Unix(int64(binary.BigEndian.Uint32(payload)), 0).UTC(), nil
	case 8:
		value := binary.BigEndian.Uint64(payload)
		return time.Unix(int64(value&0x3ffffffff), int64(value>>34)).UTC(), nil
	case 12:
		nanos := binary.BigEndian.Uint32(payload[:4])
		seconds := int64(binary.BigEndian.Uint64(payload[4:]))
		return time.Unix(seconds, int64(nanos)).UTC(), nil
	}
	return time.Time{}, fmt.Errorf("invalid timestamp length %d", len(payload))
}

func (d *msgpackDecoder) newUint8Array(b []byte) (goja.Value, error) {
	return d.construct("Uint8Array", d.vm.ToValue(d.vm.NewArrayBuffer(append([]byte{}, b...))))
}

func (d *msgpackDecoder) newDate(t time.Time) (goja.Value, error) {
	// milliseconds since the epoch, keeping sub-millisecond precision out of the way of JS Dates.
	millis := float64(t.UnixNano()) / float64(time.Millisecond)
	return d.construct("Date", d.vm.ToValue(millis))
}

func (d *msgpackDecoder) construct(name string, args ...goja.Value) (goja.Value, error) {
	constructor, ok := goja.AssertConstructor(d.vm.Get(name))
	if !ok {
		return nil, fmt.Errorf("%s is not available in the JS runtime", name)
	}
	return constructor(nil, args...)
}

func (d *msgpackDecoder) readByte() (byte, error) {
	if d.pos >= len(d.data) {
		return 0, fmt.Errorf("unexpected end of data at offset %d", d.pos)
	}
	c := d.data[d.pos]
	d.pos++
	return c, nil
}

func (d *msgpackDecoder) readBytes(length int) ([]byte, error) {
	if length < 0 || d.pos+length > len(d.data) {
		return nil, fmt.Errorf("unexpected end of data at offset %d (want %d bytes, have %d)", d.pos, length, len(d.data)-d.pos)
	}
	b := d.data[d.pos : d.pos+length]
	d.pos += length
	return b, nil
}

func (d *msgpackDecoder) readUint(size int) (uint64, error) {
	b, err := d.readBytes(size)
	if err != nil {
		return 0, err
	}
	var value uint64
	for _, c := range b {
		value = value<<8 | uint64(c)
	}
	return value, nil
}

func (d *msgpackDecoder) readLength(size int) (int, error) {
	value, err := d.readUint(size)
	if err != nil {
		return 0, err
	}
	if value > uint64(len(d.data)) {
		// a length larger than the remaining document is always malformed - fail before allocating.
		return 0, fmt.Errorf("length %d at offset %d exceeds the remaining %d bytes", value, d.pos-size, len(d.data)-d.pos)
	}
	return int(value), nil
}

// newMsgpackObject returns the "msgpack" object exposed to JS, with a lowercase decode()
// to follow JS naming conventions.
func newMsgpackObject(vm *goja.Runtime) *goja.Object {
	object := vm.NewObject()
	_ = object.Set("decode", func(call goja.FunctionCall) goja.Value {
		data, err := toBytes(vm, call.Argument(0))
		if err != nil {
			panic(vm.NewTypeError("msgpack.decode: %s", err))
		}
		value, err := decodeMsgpack(vm, data)
		if err != nil {
			panic(vm.NewGoError(fmt.Errorf("msgpack.decode: %w", err)))
		}
		return value
	})
	return object
}

// toBytes converts a JS value to bytes: an ArrayBuffer, a typed array or a DataView is used as-is,
// a string is UTF-8 encoded.
func toBytes(vm *goja.Runtime, value goja.Value) ([]byte, error) {
	if goja.IsUndefined(value) || goja.IsNull(value) {
		return nil, fmt.Errorf("no data given")
	}
	if buffer, ok := value.Export().(goja.ArrayBuffer); ok {
		return buffer.Bytes(), nil
	}
	if object, ok := value.(*goja.Object); ok {
		// typed arrays and DataViews expose their bytes via their backing buffer.
		if buffer := object.Get("buffer"); buffer != nil {
			if arrayBuffer, ok := buffer.Export().(goja.ArrayBuffer); ok {
				offset := object.Get("byteOffset").ToInteger()
				length := object.Get("byteLength").ToInteger()
				b := arrayBuffer.Bytes()
				if offset < 0 || length < 0 || offset+length > int64(len(b)) {
					return nil, fmt.Errorf("invalid view bounds")
				}
				return b[offset : offset+length], nil
			}
		}
	}
	if value.ExportType() != nil && value.ExportType().Kind().String() == "string" {
		return []byte(value.String()), nil
	}
	return nil, fmt.Errorf("expected a Uint8Array, an ArrayBuffer or a string, got %s", value.ExportType())
}
