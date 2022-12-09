package restrix

import (
	"strings"

	"github.com/gosimple/slug"
)

type namespacer struct {
	name string
}

func newNamespacer(name string) namespacer {
	return namespacer{name: "restrix:" + strings.TrimSpace(slug.Make(name))}
}

func (n namespacer) currentState() string {
	return n.name + ":current_state"
}

func (n namespacer) requestCount() string {
	return n.name + ":request_count"
}

func (n namespacer) errorCount() string {
	return n.name + ":error_count"
}

func (n namespacer) openStateTTL() string {
	return n.name + ":open_state_ttl"
}
