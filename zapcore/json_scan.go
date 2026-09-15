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

// scanJSONFieldUsage counts the resources contained in an already
// serialized JSON value, without re-evaluating the value that produced it:
//
//   - members is the total number of object members ("key": value pairs)
//     found anywhere in the document;
//   - maxDepth is the maximum nesting depth reached by objects and arrays,
//     with the document's root value at depth 0 (so "{}" or "[]" reports 1,
//     a scalar or "null" reports 0).
//
// The input is expected to be the output of encoding/json (which is always
// valid JSON). If the input is malformed, (0, 0) is returned so that a scan
// failure never causes an entry to be rejected.
func scanJSONFieldUsage(data []byte) (members, maxDepth int) {
	// Stack of open containers: '{' or '['.
	var stack []byte
	// expectKey reports whether, when the top container is an object, the
	// next string token is a member key (rather than a value).
	expectKey := false

	for i := 0; i < len(data); {
		switch data[i] {
		case ' ', '\t', '\n', '\r':
			i++
		case '{':
			stack = append(stack, '{')
			if len(stack) > maxDepth {
				maxDepth = len(stack)
			}
			expectKey = true
			i++
		case '[':
			stack = append(stack, '[')
			if len(stack) > maxDepth {
				maxDepth = len(stack)
			}
			expectKey = false
			i++
		case '}', ']':
			if len(stack) == 0 {
				return 0, 0
			}
			stack = stack[:len(stack)-1]
			expectKey = false
			i++
		case '"':
			next, ok := scanJSONStringToken(data, i)
			if !ok {
				return 0, 0
			}
			if len(stack) > 0 && stack[len(stack)-1] == '{' && expectKey {
				members++
				// The next token must be ':' followed by the value; the
				// string can't be another key until a ',' is seen.
				expectKey = false
			}
			i = next
		case ',':
			if len(stack) > 0 && stack[len(stack)-1] == '{' {
				expectKey = true
			}
			i++
		case ':':
			i++
		case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			// Skip the whole number token (including '.', 'e'/'E', '+').
			i++
			for i < len(data) && !isJSONDelimiter(data[i]) {
				i++
			}
		case 't':
			if !consumeJSONLiteral(&i, data, "true") {
				return 0, 0
			}
		case 'f':
			if !consumeJSONLiteral(&i, data, "false") {
				return 0, 0
			}
		case 'n':
			if !consumeJSONLiteral(&i, data, "null") {
				return 0, 0
			}
		default:
			return 0, 0
		}
	}

	if len(stack) != 0 {
		return 0, 0
	}
	return members, maxDepth
}

// isJSONDelimiter reports whether b ends an in-progress JSON token.
func isJSONDelimiter(b byte) bool {
	switch b {
	case ',', '}', ']', ' ', '\t', '\n', '\r':
		return true
	default:
		return false
	}
}

// scanJSONStringToken returns the index immediately after the JSON string
// starting at data[i] (which must be '"'), honoring escape sequences.
func scanJSONStringToken(data []byte, i int) (int, bool) {
	i++
	for i < len(data) {
		switch data[i] {
		case '\\':
			// Skip the escaped byte as well; this is sufficient for all
			// JSON escapes (including \uXXXX) because an escaped byte can
			// never terminate the string.
			i += 2
		case '"':
			return i + 1, true
		default:
			i++
		}
	}
	return i, false
}

// consumeJSONLiteral advances *i past literal if it matches at that position,
// reporting whether it matched.
func consumeJSONLiteral(i *int, data []byte, literal string) bool {
	start := *i
	if start+len(literal) <= len(data) && string(data[start:start+len(literal)]) == literal {
		*i = start + len(literal)
		return true
	}
	return false
}
