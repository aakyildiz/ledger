package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

// signSpec is opRequest.sign (DESIGN §7 C4): the key id the server knows the
// key by, and the Ed25519 seed (base64, 32 bytes) the driver drew.
type signSpec struct {
	KeyID string `json:"keyId"`
	Seed  string `json:"seed"`
}

func decodeSeed(seed string) (ed25519.PrivateKey, error) {
	b, err := base64.StdEncoding.DecodeString(seed)
	if err != nil {
		return nil, fmt.Errorf("seed: %w", err)
	}
	if len(b) != ed25519.SeedSize {
		return nil, fmt.Errorf("seed: %d bytes, want %d", len(b), ed25519.SeedSize)
	}

	return ed25519.NewKeyFromSeed(b), nil
}

// signApply replaces apply's unsigned batch by a signed envelope, keeping
// every other field of the request.
//
// The signature is deterministic, so a retry of the same op request resends
// identical bytes on every client. signing.Sign serializes with MarshalVT,
// whose map iteration (metadata, account types) is not ordered, so two calls
// can sign different payload bytes for one batch. signBatch therefore builds
// the same envelope as signing.Sign over a deterministic serialization;
// signing.Verify and signing.ExtractBatch accept it unchanged (sign_test.go).
func signApply(apply *servicepb.ApplyRequest, spec signSpec) error {
	batch := apply.GetUnsigned()
	if batch == nil {
		return errors.New("sign: request.unsigned is required")
	}
	if spec.KeyID == "" {
		return errors.New("sign: keyId is required")
	}
	key, err := decodeSeed(spec.Seed)
	if err != nil {
		return err
	}
	signed, err := signBatch(batch, spec.KeyID, key)
	if err != nil {
		return err
	}
	apply.Variant = &servicepb.ApplyRequest_Signed{Signed: signed}

	return nil
}

// signBatch is signing.Sign with a deterministic payload.
func signBatch(batch *servicepb.ApplyBatch, keyID string, key ed25519.PrivateKey) (*signaturepb.SignedApplyBatch, error) {
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(batch)
	if err != nil {
		return nil, fmt.Errorf("marshaling batch for signing: %w", err)
	}

	return &signaturepb.SignedApplyBatch{
		KeyId:     keyID,
		Signature: ed25519.Sign(key, payload),
		Payload:   payload,
	}, nil
}

// handlePubkey: POST /pubkey {"seed"} -> {"publicKey": "<b64>"}.
func handlePubkey(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var req struct {
		Seed string `json:"seed"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	key, err := decodeSeed(req.Seed)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	pub, ok := key.Public().(ed25519.PublicKey)
	if !ok {
		http.Error(w, "unexpected public key type", http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]string{"publicKey": base64.StdEncoding.EncodeToString(pub)})
}
