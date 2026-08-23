package grpc

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/keelab/keelith/selector"
	ggrpc "google.golang.org/grpc"
)

type discoveryClientStream struct {
	ggrpc.ClientStream

	done            selector.Done
	release         func()
	started         time.Time
	context         context.Context
	finishOnReceive bool
	completed       chan struct{}
	once            sync.Once
}

func newDiscoveryClientStream(
	ctx context.Context,
	stream ggrpc.ClientStream,
	done selector.Done,
	release func(),
	started time.Time,
	finishOnReceive bool,
) *discoveryClientStream {
	wrapped := &discoveryClientStream{
		ClientStream:    stream,
		done:            done,
		release:         release,
		started:         started,
		context:         ctx,
		finishOnReceive: finishOnReceive,
		completed:       make(chan struct{}),
	}
	go wrapped.watchContext()
	return wrapped
}

func (stream *discoveryClientStream) Header() (mapMetadata, error) {
	header, err := stream.ClientStream.Header()
	if err != nil {
		stream.finish(err)
	}
	return header, err
}

func (stream *discoveryClientStream) CloseSend() error {
	err := stream.ClientStream.CloseSend()
	if err != nil {
		stream.finish(err)
	}
	return err
}

func (stream *discoveryClientStream) SendMsg(message any) error {
	err := stream.ClientStream.SendMsg(message)
	if err != nil && !errors.Is(err, io.EOF) {
		stream.finish(err)
	}
	return err
}

func (stream *discoveryClientStream) RecvMsg(message any) error {
	err := stream.ClientStream.RecvMsg(message)
	switch {
	case errors.Is(err, io.EOF):
		stream.finish(nil)
	case err != nil:
		stream.finish(err)
	case stream.finishOnReceive:
		stream.finish(nil)
	}
	return err
}

func (stream *discoveryClientStream) finish(err error) {
	stream.once.Do(func() {
		stream.release()
		stream.done(selectionResult(
			stream.context,
			stream.started,
			err,
		))
		close(stream.completed)
	})
}

func (stream *discoveryClientStream) watchContext() {
	select {
	case <-stream.context.Done():
		stream.finish(context.Cause(stream.context))
	case <-stream.completed:
	}
}
