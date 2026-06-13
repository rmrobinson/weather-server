package grpcserver

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// mockServerStream implements grpc.ServerStream for testing the interceptor.
type mockServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockServerStream) Context() context.Context { return m.ctx }

func ctxWithMeta(key, val string) context.Context {
	md := metadata.Pairs(key, val)
	return metadata.NewIncomingContext(context.Background(), md)
}

func TestPSKStreamInterceptor_EmptyPSK_AllowsAll(t *testing.T) {
	interceptor := PSKStreamInterceptor("")
	called := false
	handler := func(srv any, _ grpc.ServerStream) error {
		called = true
		return nil
	}
	stream := &mockServerStream{ctx: context.Background()}
	if err := interceptor(nil, stream, nil, handler); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("handler should have been called")
	}
}

func TestPSKStreamInterceptor_ValidKey_Passes(t *testing.T) {
	interceptor := PSKStreamInterceptor("secret")
	called := false
	handler := func(srv any, _ grpc.ServerStream) error {
		called = true
		return nil
	}
	stream := &mockServerStream{ctx: ctxWithMeta("authorization", "psk secret")}
	if err := interceptor(nil, stream, nil, handler); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("handler should have been called for valid PSK")
	}
}

func TestPSKStreamInterceptor_InvalidKey_Rejected(t *testing.T) {
	interceptor := PSKStreamInterceptor("secret")
	handler := func(srv any, _ grpc.ServerStream) error {
		t.Error("handler should not have been called")
		return nil
	}
	stream := &mockServerStream{ctx: ctxWithMeta("authorization", "psk wrong")}
	if err := interceptor(nil, stream, nil, handler); err == nil {
		t.Error("expected error for invalid PSK")
	}
}

func TestPSKStreamInterceptor_MissingHeader_Rejected(t *testing.T) {
	interceptor := PSKStreamInterceptor("secret")
	handler := func(srv any, _ grpc.ServerStream) error {
		t.Error("handler should not have been called")
		return nil
	}
	stream := &mockServerStream{ctx: context.Background()}
	if err := interceptor(nil, stream, nil, handler); err == nil {
		t.Error("expected error for missing authorization header")
	}
}

// ── PSKUnaryInterceptor ───────────────────────────────────────────────────────

func TestPSKUnaryInterceptor_EmptyPSK_AllowsAll(t *testing.T) {
	interceptor := PSKUnaryInterceptor("")
	called := false
	handler := func(ctx context.Context, req any) (any, error) {
		called = true
		return nil, nil
	}
	if _, err := interceptor(context.Background(), nil, nil, handler); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("handler should have been called")
	}
}

func TestPSKUnaryInterceptor_ValidKey_Passes(t *testing.T) {
	interceptor := PSKUnaryInterceptor("secret")
	called := false
	handler := func(ctx context.Context, req any) (any, error) {
		called = true
		return nil, nil
	}
	ctx := ctxWithMeta("authorization", "psk secret")
	if _, err := interceptor(ctx, nil, nil, handler); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("handler should have been called for valid PSK")
	}
}

func TestPSKUnaryInterceptor_InvalidKey_Rejected(t *testing.T) {
	interceptor := PSKUnaryInterceptor("secret")
	handler := func(ctx context.Context, req any) (any, error) {
		t.Error("handler should not have been called")
		return nil, nil
	}
	ctx := ctxWithMeta("authorization", "psk wrong")
	if _, err := interceptor(ctx, nil, nil, handler); err == nil {
		t.Error("expected error for invalid PSK")
	}
}

func TestPSKUnaryInterceptor_MissingHeader_Rejected(t *testing.T) {
	interceptor := PSKUnaryInterceptor("secret")
	handler := func(ctx context.Context, req any) (any, error) {
		t.Error("handler should not have been called")
		return nil, nil
	}
	if _, err := interceptor(context.Background(), nil, nil, handler); err == nil {
		t.Error("expected error for missing authorization header")
	}
}
