package genericrpc

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/cloudwego/gopkg/bufiox"
	"github.com/cloudwego/kitex/pkg/remote"
)

type bufferedTLSConnection struct {
	net.Conn
	reader *bufiox.DefaultReader
	writer *bufiox.DefaultWriter
	closed atomic.Bool
}

func newBufferedTLSConnection(connection net.Conn) *bufferedTLSConnection {
	return &bufferedTLSConnection{
		Conn:   connection,
		reader: bufiox.NewDefaultReader(connection),
		writer: bufiox.NewDefaultWriter(connection),
	}
}

func (connection *bufferedTLSConnection) Reader() *bufiox.DefaultReader {
	return connection.reader
}

func (connection *bufferedTLSConnection) Writer() *bufiox.DefaultWriter {
	return connection.writer
}

func (connection *bufferedTLSConnection) Read(buffer []byte) (int, error) {
	return connection.reader.Read(buffer)
}

func (connection *bufferedTLSConnection) Close() error {
	if !connection.closed.CompareAndSwap(false, true) {
		return net.ErrClosed
	}
	connection.reader.Release(nil)
	return connection.Conn.Close()
}

func tlsTransportDialer(config validatedConfig) remote.Dialer {
	return remote.SynthesizedDialer{
		DialFunc: func(
			network string,
			address string,
			timeout time.Duration,
		) (net.Conn, error) {
			dialer := &net.Dialer{Timeout: timeout}
			tlsConfig := config.tlsConfig.Clone()
			if tlsConfig.ServerName == "" {
				host, _, err := net.SplitHostPort(address)
				if err != nil {
					return nil, fmt.Errorf(
						"kitex generic: TLS address is malformed",
					)
				}
				tlsConfig.ServerName = host
			}
			connection, err := tls.DialWithDialer(
				dialer,
				network,
				address,
				tlsConfig,
			)
			if err != nil {
				return nil, fmt.Errorf(
					"kitex generic: establish TLS transport: %w",
					err,
				)
			}
			return newBufferedTLSConnection(connection), nil
		},
	}
}
