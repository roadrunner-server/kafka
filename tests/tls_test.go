package tests

import (
	"testing"

	"tests/helpers"
)

const (
	tlsAddr       = "127.0.0.1:6002"
	tlsRootCAAddr = "127.0.0.1:6003"
)

// TestTLS covers mutual TLS against the broker's SSL listener, with the client
// key pair and the root CA configured explicitly.
func TestTLS(t *testing.T) {
	helpers.CleanupTopics(t, "test-1-tls")

	rr, _ := boot(t, "configs/.rr-kafka-tls.yaml", tlsAddr)

	helpers.PushToPipe("test-1-tls", false, tlsAddr)(t)

	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.DestroyPipelines(tlsAddr, "test-1-tls")(t)

	rr.RequireLogCount(t, "job was pushed successfully", 1)
	rr.RequireLogCount(t, "job was processed successfully", 1)
}

// TestTLSRootCA covers the same handshake configured through the root CA only.
func TestTLSRootCA(t *testing.T) {
	helpers.CleanupTopics(t, "test-1-tls-root-ca")

	rr, _ := boot(t, "configs/.rr-kafka-tls-root-ca.yaml", tlsRootCAAddr)

	helpers.PushToPipe("test-1-tls-root-ca", false, tlsRootCAAddr)(t)

	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.DestroyPipelines(tlsRootCAAddr, "test-1-tls-root-ca")(t)

	rr.RequireLogCount(t, "job was processed successfully", 1)
}
