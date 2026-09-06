// Package promutil is shared Prometheus plumbing for the keel packages:
// namespaced registration with the same option shape everywhere.
package promutil

import (
	"fmt"
	"regexp"

	"github.com/prometheus/client_golang/prometheus"
)

var _reNamespace = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// ValidateNamespace reports whether ns is a legal metric-name prefix.
func ValidateNamespace(ns string) error {
	if !_reNamespace.MatchString(ns) {
		return fmt.Errorf("WithNamespace(%q): not a valid metric name prefix", ns)
	}
	return nil
}

// Register registers every collector with reg under namespace + "_". It is
// all or nothing: on the first registration error, typically
// prometheus.AlreadyRegisteredError on a duplicate, the collectors already
// registered by this call are unregistered again and the error is returned,
// so a failed Register leaves reg as it found it.
func Register(reg prometheus.Registerer, namespace string, collectors ...prometheus.Collector) error {
	reg = prometheus.WrapRegistererWithPrefix(namespace+"_", reg)
	for i, c := range collectors {
		if err := reg.Register(c); err != nil {
			for _, done := range collectors[:i] {
				reg.Unregister(done)
			}
			return err
		}
	}
	return nil
}
