package grpc

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"

	"github.com/keelab/keelith/selector"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

var (
	// ErrInsecureEndpoint reports a grpc:// endpoint rejected by dial policy.
	ErrInsecureEndpoint = errors.New(
		"grpc transport: insecure endpoint is not allowed",
	)
)

// NodeDialerConfig configures the default grpc-go connection factory.
//
// TLSConfig is cloned for every grpcs:// connection. DialOptions are copied;
// DisableRetry, validated Keepalive, and transport credentials are appended
// afterward so application options cannot weaken the profile's connection
// policy. DisableRetry blocks service-configured retries, but grpc-go may still
// transparently retry a call that was not processed by the remote server.
// grpc-go v1.82.1 does not implement concurrent hedging, so this option does
// not establish independently tested hedging semantics.
type NodeDialerConfig struct {
	TLSConfig     *tls.Config
	AllowInsecure bool
	// DisableRetry blocks service-configured replay after remote processing begins.
	DisableRetry bool
	DialOptions  []ggrpc.DialOption
	Keepalive    *ClientKeepaliveConfig
}

// NewNodeDialer creates a DialFunc for discovered grpc:// and grpcs:// nodes.
//
// grpc:// is rejected unless AllowInsecure is explicitly enabled. grpcs://
// uses system roots by default or a clone of TLSConfig when supplied.
func NewNodeDialer(config NodeDialerConfig) (DialFunc, error) {
	if config.TLSConfig != nil &&
		config.TLSConfig.MinVersion != 0 &&
		config.TLSConfig.MinVersion < tls.VersionTLS12 {
		return nil, fmt.Errorf(
			"%w: TLS minimum version must be TLS 1.2 or newer",
			ErrInvalidOption,
		)
	}
	options := append([]ggrpc.DialOption(nil), config.DialOptions...)
	if config.DisableRetry {
		options = append(options, ggrpc.WithDisableRetry())
	}
	if config.Keepalive != nil {
		if err := validateClientKeepalive(*config.Keepalive); err != nil {
			return nil, fmt.Errorf(
				"%w: client keepalive: %w",
				ErrInvalidOption,
				err,
			)
		}
		keepaliveConfig := *config.Keepalive
		options = append(
			options,
			ggrpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                keepaliveConfig.PingInterval,
				Timeout:             keepaliveConfig.PingTimeout,
				PermitWithoutStream: keepaliveConfig.PermitWithoutStream,
			}),
		)
	}
	var tlsTemplate *tls.Config
	if config.TLSConfig != nil {
		tlsTemplate = config.TLSConfig.Clone()
	}
	return func(
		ctx context.Context,
		node selector.Node,
	) (*ggrpc.ClientConn, error) {
		if ctx == nil {
			return nil, ErrNilContext
		}
		if cause := context.Cause(ctx); cause != nil {
			return nil, cause
		}
		endpoint, err := parseGRPCEndpoint(node)
		if err != nil {
			return nil, err
		}
		dialOptions := append([]ggrpc.DialOption(nil), options...)
		switch endpoint.Scheme {
		case "grpc":
			if !config.AllowInsecure {
				return nil, fmt.Errorf(
					"%w: node %q",
					ErrInsecureEndpoint,
					node.ID(),
				)
			}
			dialOptions = append(
				dialOptions,
				ggrpc.WithTransportCredentials(insecure.NewCredentials()),
			)
		case "grpcs":
			tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
			if tlsTemplate != nil {
				tlsConfig = tlsTemplate.Clone()
				if tlsConfig.MinVersion == 0 {
					tlsConfig.MinVersion = tls.VersionTLS12
				}
			}
			dialOptions = append(
				dialOptions,
				ggrpc.WithTransportCredentials(
					credentials.NewTLS(tlsConfig),
				),
			)
		default:
			return nil, fmt.Errorf("%w: %q", ErrInvalidEndpoint, node.Endpoint())
		}
		connection, err := ggrpc.NewClient(endpoint.Host, dialOptions...)
		if err != nil {
			return nil, fmt.Errorf(
				"grpc transport: dial node %q: %w",
				node.ID(),
				err,
			)
		}
		return connection, nil
	}, nil
}
