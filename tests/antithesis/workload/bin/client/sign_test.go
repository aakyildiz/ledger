package main

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func testSeed() string {
	return base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, ed25519.SeedSize))
}

// A batch with many metadata entries: map order would vary between
// serializations if the payload were not deterministic.
func signRequestJSON() []byte {
	var md []string
	for i := range 20 {
		md = append(md, fmt.Sprintf(`"k%d":{"stringValue":"v%d"}`, i, i))
	}

	return []byte(`{"unsigned":{"idempotencyKey":"r-1-k1","requests":[{"saveLedgerMetadata":{"ledger":"r-sign-1-1","metadata":{` +
		strings.Join(md, ",") + `}}}]},"skipResponse":true}`)
}

func TestSignRoundTrip(t *testing.T) {
	t.Parallel()
	spec := &signSpec{KeyID: "key-1", Seed: testSeed()}
	_, in, err := prepareOp(opRequest{Service: "bucket", Method: "Apply", Request: signRequestJSON(), Sign: spec})
	require.NoError(t, err)
	apply := in.(*servicepb.ApplyRequest)
	signed := apply.GetSigned()
	require.NotNil(t, signed)
	require.Nil(t, apply.GetUnsigned())
	require.True(t, apply.GetSkipResponse(), "other request fields are kept")
	require.Equal(t, "key-1", signed.GetKeyId())

	key, err := decodeSeed(spec.Seed)
	require.NoError(t, err)
	require.NoError(t, signing.Verify(signed, key.Public().(ed25519.PublicKey)))
	batch, err := signing.ExtractBatch(signed)
	require.NoError(t, err)

	var want servicepb.ApplyRequest
	require.NoError(t, unmarshal.Unmarshal(signRequestJSON(), &want))
	require.True(t, proto.Equal(want.GetUnsigned(), batch), "the signed payload is the unsigned batch")

	// Deterministic: every signing of the same request sends the same bytes.
	for range 20 {
		_, again, err := prepareOp(opRequest{Service: "bucket", Method: "Apply", Request: signRequestJSON(), Sign: spec})
		require.NoError(t, err)
		require.Equal(t, signed.GetPayload(), again.(*servicepb.ApplyRequest).GetSigned().GetPayload())
		require.Equal(t, signed.GetSignature(), again.(*servicepb.ApplyRequest).GetSigned().GetSignature())
	}

	// A different key does not verify.
	other := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{8}, ed25519.SeedSize))
	require.Error(t, signing.Verify(signed, other.Public().(ed25519.PublicKey)))
}

func TestSignRejectsMisuse(t *testing.T) {
	t.Parallel()
	spec := &signSpec{KeyID: "k", Seed: testSeed()}
	_, _, err := prepareOp(opRequest{Service: "bucket", Method: "GetLedger", Request: []byte(`{}`), Sign: spec})
	require.ErrorContains(t, err, "only valid on bucket Apply")
	_, _, err = prepareOp(opRequest{Service: "bucket", Method: "Apply", Request: []byte(`{}`), Sign: spec})
	require.ErrorContains(t, err, "unsigned is required")
	_, _, err = prepareOp(opRequest{Service: "bucket", Method: "Apply", Request: signRequestJSON(), Sign: &signSpec{KeyID: "k", Seed: "AAAA"}})
	require.ErrorContains(t, err, "seed")
	_, _, err = prepareOp(opRequest{Service: "bucket", Method: "Apply", Request: signRequestJSON(), Sign: &signSpec{Seed: testSeed()}})
	require.ErrorContains(t, err, "keyId")
}

func TestPubkey(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	handlePubkey(rec, httptest.NewRequest(http.MethodPost, "/pubkey", strings.NewReader(`{"seed":"`+testSeed()+`"}`)))
	require.Equal(t, http.StatusOK, rec.Code)
	var resp struct {
		PublicKey string `json:"publicKey"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	key, err := decodeSeed(testSeed())
	require.NoError(t, err)
	require.Equal(t, base64.StdEncoding.EncodeToString(key.Public().(ed25519.PublicKey)), resp.PublicKey)

	rec = httptest.NewRecorder()
	handlePubkey(rec, httptest.NewRequest(http.MethodPost, "/pubkey", strings.NewReader(`{"seed":"AAAA"}`)))
	require.Equal(t, http.StatusBadRequest, rec.Code)
}
