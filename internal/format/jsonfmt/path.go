package jsonfmt

import (
	"fmt"
	"strconv"
	"strings"
)

// seg is one path segment: an object key or a 0-based array index.
type seg struct {
	key     string
	index   int
	isIndex bool
}

// parsePath parses the JSON path dialect: dot-separated bare keys, ["quoted
// keys"] for keys containing metacharacters, and [n] 0-based array indexes.
// Examples: patient.name[0].given · entry[1].resource.id · ["odd.key"].x
func parsePath(p string) ([]seg, error) {
	if p == "" {
		return nil, fmt.Errorf("json: empty path")
	}
	var segs []seg
	i := 0
	for i < len(p) {
		switch {
		case p[i] == '[':
			s, n, err := parseBracket(p[i:])
			if err != nil {
				return nil, fmt.Errorf("json: invalid path %q: %w", p, err)
			}
			segs = append(segs, s)
			i += n
		case p[i] == '.':
			if len(segs) == 0 {
				return nil, fmt.Errorf("json: invalid path %q: leading '.'", p)
			}
			i++
			key, n, err := parseBareKey(p[i:])
			if err != nil {
				return nil, fmt.Errorf("json: invalid path %q: %w", p, err)
			}
			segs = append(segs, seg{key: key})
			i += n
		default:
			if len(segs) > 0 {
				return nil, fmt.Errorf("json: invalid path %q: expected '.' or '[' at offset %d", p, i)
			}
			key, n, err := parseBareKey(p[i:])
			if err != nil {
				return nil, fmt.Errorf("json: invalid path %q: %w", p, err)
			}
			segs = append(segs, seg{key: key})
			i += n
		}
	}
	return segs, nil
}

// parseBareKey consumes a key up to the next '.' or '['.
func parseBareKey(p string) (string, int, error) {
	n := strings.IndexAny(p, ".[")
	if n == -1 {
		n = len(p)
	}
	if n == 0 {
		return "", 0, fmt.Errorf("empty key")
	}
	return p[:n], n, nil
}

// parseBracket consumes a [n] index or a ["key"]/['key'] quoted key,
// returning the segment and the number of bytes consumed.
func parseBracket(p string) (seg, int, error) {
	if len(p) < 2 {
		return seg{}, 0, fmt.Errorf("unterminated '['")
	}
	if q := p[1]; q == '"' || q == '\'' {
		var b strings.Builder
		i := 2
		for i < len(p) && p[i] != q {
			if p[i] == '\\' && i+1 < len(p) {
				i++
			}
			b.WriteByte(p[i])
			i++
		}
		if i >= len(p) || i+1 >= len(p) || p[i+1] != ']' {
			return seg{}, 0, fmt.Errorf("unterminated quoted key")
		}
		return seg{key: b.String()}, i + 2, nil
	}
	end := strings.IndexByte(p, ']')
	if end == -1 {
		return seg{}, 0, fmt.Errorf("unterminated '['")
	}
	idx, err := strconv.Atoi(p[1:end])
	if err != nil || idx < 0 || (len(p[1:end]) > 1 && p[1] == '0') {
		return seg{}, 0, fmt.Errorf("invalid array index %q (0-based integer)", p[1:end])
	}
	return seg{index: idx, isIndex: true}, end + 1, nil
}
