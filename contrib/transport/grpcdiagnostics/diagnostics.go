// Package grpcdiagnostics provides explicitly enabled local-only gRPC
// reflection and administration services.
package grpcdiagnostics

import (
	kgrpc "github.com/keelab/keelith/transport/grpc"
	grpcadmin "google.golang.org/grpc/admin"
	grpcreflection "google.golang.org/grpc/reflection"
)

// Reflection returns a loopback/Unix-only server reflection option.
func Reflection() kgrpc.ServerOption {
	return kgrpc.WithLocalDiagnostic(
		"reflection",
		func(registrar kgrpc.LocalDiagnosticServer) (func(), error) {
			grpcreflection.Register(registrar)
			return nil, nil
		},
	)
}

// AdminServices returns a loopback/Unix-only Channelz and CSDS option.
//
// grpc-go currently marks its aggregate admin registration API experimental;
// this adapter therefore remains in contrib.
func AdminServices() kgrpc.ServerOption {
	return kgrpc.WithLocalDiagnostic(
		"admin",
		func(registrar kgrpc.LocalDiagnosticServer) (func(), error) {
			return grpcadmin.Register(registrar)
		},
	)
}
