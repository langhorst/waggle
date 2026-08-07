package xmlfmt

import (
	"fmt"
	"strconv"
	"strings"
)

// step is one path step: an element name with an optional 1-based
// occurrence predicate, or a final attribute selector.
type step struct {
	name       string
	occurrence int // 0 = wildcard (all same-name siblings)
	isAttr     bool
}

// parsePath parses the XPath-flavored dialect: slash-separated element
// steps with 1-based [n] predicates and @attr as the final step —
// "Patient/name[1]/family", "Patient/@id", "entry/resource/id/@value".
// A leading slash is accepted (XPath habit) and means the same thing.
func parsePath(p string) ([]step, error) {
	orig := p
	p = strings.TrimPrefix(p, "/")
	if p == "" {
		return nil, fmt.Errorf("xml: empty path")
	}
	parts := strings.Split(p, "/")
	steps := make([]step, 0, len(parts))
	for i, part := range parts {
		if part == "" {
			return nil, fmt.Errorf("xml: invalid path %q: empty step", orig)
		}
		if strings.HasPrefix(part, "@") {
			name := part[1:]
			if name == "" || strings.ContainsAny(name, "[]@") {
				return nil, fmt.Errorf("xml: invalid path %q: bad attribute step %q", orig, part)
			}
			if i != len(parts)-1 {
				return nil, fmt.Errorf("xml: invalid path %q: @%s must be the final step", orig, name)
			}
			steps = append(steps, step{name: name, isAttr: true})
			continue
		}
		s := step{name: part}
		if open := strings.IndexByte(part, '['); open != -1 {
			if !strings.HasSuffix(part, "]") {
				return nil, fmt.Errorf("xml: invalid path %q: unterminated '[' in %q", orig, part)
			}
			idx := part[open+1 : len(part)-1]
			n, err := strconv.Atoi(idx)
			if err != nil || n < 1 || (len(idx) > 1 && idx[0] == '0') {
				return nil, fmt.Errorf("xml: invalid path %q: occurrence %q (1-based, like XPath)", orig, idx)
			}
			s.name, s.occurrence = part[:open], n
		}
		if s.name == "" || strings.ContainsAny(s.name, "[]@") {
			return nil, fmt.Errorf("xml: invalid path %q: bad step %q", orig, part)
		}
		steps = append(steps, s)
	}
	return steps, nil
}
