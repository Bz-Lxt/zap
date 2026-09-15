// Copyright (c) 2026 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package zapcore

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScanJSONFieldUsage(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		members  int
		maxDepth int
	}{
		{name: "null", json: `null`, members: 0, maxDepth: 0},
		{name: "number", json: `1.5e-3`, members: 0, maxDepth: 0},
		{name: "string", json: `"foo"`, members: 0, maxDepth: 0},
		{name: "bool", json: `true`, members: 0, maxDepth: 0},
		{name: "empty object", json: `{}`, members: 0, maxDepth: 1},
		{name: "empty array", json: `[]`, members: 0, maxDepth: 1},
		{name: "flat object", json: `{"a":1,"b":2}`, members: 2, maxDepth: 1},
		{name: "nested object", json: `{"a":{"b":{"c":1}}}`, members: 3, maxDepth: 3},
		{name: "object in array", json: `[{"a":1},{"b":2}]`, members: 2, maxDepth: 2},
		{name: "array value", json: `{"a":[1,2,3]}`, members: 1, maxDepth: 2},
		{name: "mixed", json: `{"a":"b","c":{"d":true},"e":[1,2]}`, members: 4, maxDepth: 2},
		{name: "escaped string", json: `{"k":"v\",\"w":[1]}`, members: 1, maxDepth: 2},
		{name: "whitespace", json: "  {\n \"a\": 1 \t}\r\n", members: 1, maxDepth: 1},
		{name: "malformed unclosed", json: `{"a":1`, members: 0, maxDepth: 0},
		{name: "malformed token", json: `{"a":x}`, members: 0, maxDepth: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			members, maxDepth := scanJSONFieldUsage([]byte(tt.json))
			assert.Equal(t, tt.members, members, "members mismatch")
			assert.Equal(t, tt.maxDepth, maxDepth, "maxDepth mismatch")
		})
	}
}

func fieldLimitTestConfig() EncoderConfig {
	return EncoderConfig{
		MessageKey: "msg",
	}
}

func TestFieldLimitDisabledLeavesOutputUnchanged(t *testing.T) {
	cfg := fieldLimitTestConfig()
	enc := newJSONEncoder(cfg, false)

	buf, err := enc.EncodeEntry(
		Entry{Level: InfoLevel, Message: "m"},
		[]Field{
			{Key: "a", Type: StringType, String: "1"},
			{Key: "b", Type: Int64Type, Integer: 2},
		},
	)
	require.NoError(t, err)
	defer buf.Free()
	assert.Equal(t, `{"msg":"m","a":"1","b":2}`+"\n", buf.String())
}

func TestFieldLimitUnderLimitIsByteIdentical(t *testing.T) {
	sampleFields := func() []Field {
		return []Field{
			{Key: "s", Type: StringType, String: "v"},
			{Key: "n", Type: Int64Type, Integer: 7},
			{Key: "ns", Type: NamespaceType},
			{Key: "inner", Type: StringType, String: "z"},
			{Key: "o", Type: ObjectMarshalerType, Interface: ObjectMarshalerFunc(func(oe ObjectEncoder) error {
				oe.AddString("a", "1")
				oe.AddInt("b", 2)
				return nil
			})},
			{Key: "arr", Type: ArrayMarshalerType, Interface: ArrayMarshalerFunc(func(ae ArrayEncoder) error {
				ae.AppendInt(1)
				ae.AppendString("two")
				return nil
			})},
			{Key: "r", Type: ReflectType, Interface: map[string]interface{}{
				"x": 1,
				"y": []int{1, 2},
			}},
			{Type: InlineMarshalerType, Interface: ObjectMarshalerFunc(func(oe ObjectEncoder) error {
				oe.AddBool("inlined", true)
				return nil
			})},
		}
	}

	encode := func(cfg EncoderConfig) string {
		enc := newJSONEncoder(cfg, false)
		// Inherited context accumulated via Core.With.
		addFields(enc, []Field{{Key: "base", Type: StringType, String: "b"}})
		buf, err := enc.EncodeEntry(
			Entry{Level: InfoLevel, Message: "m"},
			sampleFields(),
		)
		require.NoError(t, err)
		defer buf.Free()
		return buf.String()
	}

	unlimited := fieldLimitTestConfig()

	generous := fieldLimitTestConfig()
	generous.MaxFields = 100
	generous.MaxDepth = 100
	generous.OnFieldLimit = func(Entry, FieldLimitError) {
		t.Errorf("OnFieldLimit must not fire for entries within the limits")
	}

	assert.Equal(t, encode(unlimited), encode(generous))
}

func TestFieldLimitCountsDuplicateKeys(t *testing.T) {
	cfg := fieldLimitTestConfig()
	cfg.MaxFields = 2
	enc := newJSONEncoder(cfg, false)

	_, err := enc.EncodeEntry(
		Entry{Message: "m"},
		[]Field{
			{Key: "k", Type: StringType, String: "a"},
			{Key: "k", Type: StringType, String: "b"},
			{Key: "k", Type: StringType, String: "c"},
		},
	)
	var limitErr *FieldLimitError
	require.ErrorAs(t, err, &limitErr)
	assert.Equal(t, FieldLimitFieldsExceeded, limitErr.Reason)
	assert.Equal(t, 2, limitErr.Limit)
	assert.Equal(t, 3, limitErr.Fields)
	assert.Equal(t, "k", limitErr.Key)
}

func TestFieldLimitMetadataNotCounted(t *testing.T) {
	cfg := fieldLimitTestConfig()
	cfg.LevelKey = "level"
	cfg.EncodeLevel = LowercaseLevelEncoder
	cfg.TimeKey = "ts"
	cfg.EncodeTime = EpochTimeEncoder
	cfg.StacktraceKey = "stack"
	cfg.MaxFields = 1
	enc := newJSONEncoder(cfg, false)

	// Exactly one business field is allowed even though the entry also
	// carries level, time, message, and a stacktrace.
	buf, err := enc.EncodeEntry(
		Entry{Level: InfoLevel, Message: "m", Stack: "goroutine 1 [running]:"},
		[]Field{{Key: "a", Type: StringType, String: "1"}},
	)
	require.NoError(t, err)
	defer buf.Free()
	assert.Contains(t, buf.String(), `"a":"1"`)
}

func TestFieldLimitInheritedBaseline(t *testing.T) {
	cfg := fieldLimitTestConfig()
	cfg.MaxFields = 2
	enc := newJSONEncoder(cfg, false)
	require.True(t, enc.counting)

	// Simulate Core.With accumulating inherited context.
	addFields(enc, []Field{
		{Key: "a", Type: StringType, String: "1"},
		{Key: "b", Type: StringType, String: "2"},
	})
	assert.Equal(t, 2, enc.fieldCount)
	require.Nil(t, enc.rejected)

	// One more entry-site field exceeds the limit.
	_, err := enc.EncodeEntry(
		Entry{Message: "m"},
		[]Field{{Key: "c", Type: StringType, String: "3"}},
	)
	var limitErr *FieldLimitError
	require.ErrorAs(t, err, &limitErr)
	assert.Equal(t, FieldLimitFieldsExceeded, limitErr.Reason)
	assert.Equal(t, 3, limitErr.Fields)

	// The persistent encoder is untouched, so it still encodes entries that
	// fit within the inherited baseline.
	buf, err := enc.EncodeEntry(Entry{Message: "m"}, nil)
	require.NoError(t, err)
	defer buf.Free()
	assert.Contains(t, buf.String(), `"a":"1"`)
	assert.Contains(t, buf.String(), `"b":"2"`)
}

func TestFieldLimitDepthTracking(t *testing.T) {
	cfg := fieldLimitTestConfig()
	cfg.MaxDepth = 1
	enc := newJSONEncoder(cfg, false)

	// One level of nesting is allowed: an object with a primitive field.
	require.NoError(t, enc.AddObject("o", ObjectMarshalerFunc(func(oe ObjectEncoder) error {
		oe.AddString("inner", "v")
		return nil
	})))
	require.Nil(t, enc.rejected)
	require.Equal(t, 0, enc.depth)

	// Two levels are not.
	err := enc.AddObject("deep", ObjectMarshalerFunc(func(oe ObjectEncoder) error {
		return oe.AddObject("nested", ObjectMarshalerFunc(func(oe2 ObjectEncoder) error {
			oe2.AddString("x", "y")
			return nil
		}))
	}))
	var limitErr *FieldLimitError
	require.ErrorAs(t, err, &limitErr)
	assert.Equal(t, FieldLimitDepthExceeded, limitErr.Reason)
	assert.Equal(t, 1, limitErr.Limit)

	// Arrays add a nesting level too; an object inside an array is depth 2.
	enc2 := newJSONEncoder(cfg, false)
	err = enc2.AddArray("arr", ArrayMarshalerFunc(func(ae ArrayEncoder) error {
		return ae.AppendObject(ObjectMarshalerFunc(func(oe ObjectEncoder) error {
			oe.AddString("x", "y")
			return nil
		}))
	}))
	require.ErrorAs(t, err, &limitErr)

	// A primitive array at depth 1 is allowed.
	enc3 := newJSONEncoder(cfg, false)
	require.NoError(t, enc3.AddArray("xs", ArrayMarshalerFunc(func(ae ArrayEncoder) error {
		ae.AppendInt(1)
		ae.AppendInt(2)
		return nil
	})))
}

func TestFieldLimitInlineDoesNotAddDepth(t *testing.T) {
	cfg := fieldLimitTestConfig()
	cfg.MaxFields = 2
	enc := newJSONEncoder(cfg, false)

	// The inlined object contributes two fields but no nesting level, so
	// with no depth configured encoding succeeds.
	buf, err := enc.EncodeEntry(
		Entry{Message: "m"},
		[]Field{{
			Type: InlineMarshalerType,
			Interface: ObjectMarshalerFunc(func(oe ObjectEncoder) error {
				oe.AddString("a", "1")
				oe.AddString("b", "2")
				return nil
			}),
		}},
	)
	require.NoError(t, err)
	defer buf.Free()
	assert.Equal(t, `{"msg":"m","a":"1","b":"2"}`+"\n", buf.String())
}

func TestFieldLimitReflectedAccounting(t *testing.T) {
	t.Run("members counted", func(t *testing.T) {
		cfg := fieldLimitTestConfig()
		cfg.MaxFields = 4 // one for the key plus three reflected members
		enc := newJSONEncoder(cfg, false)
		buf, err := enc.EncodeEntry(
			Entry{Message: "m"},
			[]Field{{Key: "m", Type: ReflectType, Interface: map[string]int{"a": 1, "b": 2, "c": 3}}},
		)
		require.NoError(t, err)
		defer buf.Free()
	})

	t.Run("members exceed", func(t *testing.T) {
		cfg := fieldLimitTestConfig()
		cfg.MaxFields = 3
		enc := newJSONEncoder(cfg, false)
		_, err := enc.EncodeEntry(
			Entry{Message: "m"},
			[]Field{{Key: "m", Type: ReflectType, Interface: map[string]int{"a": 1, "b": 2, "c": 3}}},
		)
		var limitErr *FieldLimitError
		require.ErrorAs(t, err, &limitErr)
		assert.Equal(t, FieldLimitFieldsExceeded, limitErr.Reason)
		assert.Equal(t, 4, limitErr.Fields)
		assert.Equal(t, "m", limitErr.Key)
	})

	t.Run("reflected depth", func(t *testing.T) {
		cfg := fieldLimitTestConfig()
		cfg.MaxDepth = 1
		enc := newJSONEncoder(cfg, false)
		_, err := enc.EncodeEntry(
			Entry{Message: "m"},
			[]Field{{
				Key:       "m",
				Type:      ReflectType,
				Interface: map[string]any{"a": map[string]int{"b": 1}},
			}},
		)
		var limitErr *FieldLimitError
		require.ErrorAs(t, err, &limitErr)
		assert.Equal(t, FieldLimitDepthExceeded, limitErr.Reason)
	})

	t.Run("nil reflection counts only the key", func(t *testing.T) {
		cfg := fieldLimitTestConfig()
		cfg.MaxFields = 1
		enc := newJSONEncoder(cfg, false)
		buf, err := enc.EncodeEntry(
			Entry{Message: "m"},
			[]Field{{Key: "k", Type: ReflectType, Interface: nil}},
		)
		require.NoError(t, err)
		defer buf.Free()
		assert.Contains(t, buf.String(), `"k":null`)
	})
}

func TestFieldLimitErrorSentinel(t *testing.T) {
	err := &FieldLimitError{Reason: FieldLimitFieldsExceeded}
	assert.True(t, errors.Is(err, ErrFieldLimitExceeded))
	assert.Contains(t, err.Error(), "business fields")

	err = &FieldLimitError{Reason: FieldLimitDepthExceeded}
	assert.Contains(t, err.Error(), "nesting depth")
}

func TestFieldLimitCallbackInvokedOnce(t *testing.T) {
	cfg := fieldLimitTestConfig()
	cfg.MaxFields = 1
	calls := 0
	cfg.OnFieldLimit = func(_ Entry, e FieldLimitError) {
		calls++
		assert.Equal(t, FieldLimitFieldsExceeded, e.Reason)
	}
	enc := newJSONEncoder(cfg, false)

	for i := 0; i < 3; i++ {
		_, err := enc.EncodeEntry(
			Entry{Message: "m"},
			[]Field{
				{Key: "a", Type: StringType, String: "1"},
				{Key: "b", Type: StringType, String: "2"},
			},
		)
		require.Error(t, err)
	}
	assert.Equal(t, 3, calls)
}
