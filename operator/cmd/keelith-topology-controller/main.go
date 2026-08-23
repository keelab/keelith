// Command keelith-topology-controller runs the namespaced TopologyRevision
// reconciler. It coordinates Kubernetes objects and never proxies data-plane
// traffic.
package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/keelab/operator"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	publicKeyEnvironment     = "KEELITH_TOPOLOGY_PUBLIC_KEY"
	allowUnsignedEnvironment = "KEELITH_TOPOLOGY_ALLOW_UNSIGNED"
	namespaceEnvironment     = "POD_NAMESPACE"
)

func main() {
	os.Exit(mainExitCode())
}

func mainExitCode() int {
	ctx, cancel := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer cancel()
	if err := run(ctx, os.Args[1:]); err != nil &&
		!errors.Is(err, context.Canceled) {
		log.Printf("topology controller stopped: %v", err)
		return 1
	}
	return 0
}

func run(ctx context.Context, arguments []string) error {
	flags := flag.NewFlagSet("keelith-topology-controller", flag.ContinueOnError)
	namespace := flags.String(
		"namespace",
		strings.TrimSpace(os.Getenv(namespaceEnvironment)),
		"single namespace to watch",
	)
	healthAddress := flags.String(
		"health-address",
		":8081",
		"health listener address",
	)
	kubeconfig := flags.String(
		"kubeconfig",
		"",
		"optional out-of-cluster kubeconfig",
	)
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	*namespace = strings.TrimSpace(*namespace)
	*healthAddress = strings.TrimSpace(*healthAddress)
	*kubeconfig = strings.TrimSpace(*kubeconfig)
	if *namespace == "" || *healthAddress == "" {
		return operator.ErrInvalidConfig
	}
	allowUnsigned, publicKey, err := loadTrustPolicy()
	if err != nil {
		return err
	}
	restConfig, err := loadRESTConfig(*kubeconfig)
	if err != nil {
		return err
	}
	restConfig.UserAgent = "keelith-topology-controller/v1alpha1"
	restConfig.QPS = 20
	restConfig.Burst = 30
	kube, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return err
	}
	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return err
	}
	reconciler, err := operator.NewReconciler(operator.Config{
		Kubernetes: kube, Dynamic: dynamicClient,
		PublicKey: publicKey, AllowUnsigned: allowUnsigned,
	})
	if err != nil {
		return err
	}
	var ready atomic.Bool
	server := healthServer(*healthAddress, &ready)
	serverErrors := make(chan error, 1)
	go func() {
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serverErrors <- err
	}()
	controllerErrors := make(chan error, 1)
	go func() {
		controllerErrors <- operator.RunController(ctx, operator.ControllerConfig{
			Dynamic: dynamicClient, Reconciler: reconciler, Namespace: *namespace,
			Ready: func() { ready.Store(true) },
			OnError: func(err error) {
				switch {
				case errors.Is(err, operator.ErrInvalidRevision):
					log.Print("topology reconcile: revision rejected")
				case errors.Is(err, operator.ErrResourceConflict):
					log.Print("topology reconcile: managed resource conflict")
				default:
					log.Print("topology reconcile: control-plane failure")
				}
			},
		})
	}()
	var result error
	select {
	case <-ctx.Done():
		result = context.Cause(ctx)
	case result = <-serverErrors:
	case result = <-controllerErrors:
	}
	ready.Store(false)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return errors.Join(result, server.Shutdown(shutdownCtx))
}

func loadTrustPolicy() (bool, ed25519.PublicKey, error) {
	allowText := strings.TrimSpace(os.Getenv(allowUnsignedEnvironment))
	publicKeyText := strings.TrimSpace(os.Getenv(publicKeyEnvironment))
	switch allowText {
	case "true":
		if publicKeyText != "" {
			return false, nil, fmt.Errorf(
				"%w: unsigned and public key are mutually exclusive",
				operator.ErrInvalidConfig,
			)
		}
		return true, nil, nil
	case "":
	default:
		return false, nil, operator.ErrInvalidConfig
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(publicKeyText)
	if err != nil || len(decoded) != ed25519.PublicKeySize {
		return false, nil, operator.ErrInvalidConfig
	}
	return false, ed25519.PublicKey(decoded), nil
}

func loadRESTConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}

func healthServer(address string, ready *atomic.Bool) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/readyz", func(writer http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(writer, "not ready", http.StatusServiceUnavailable)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	})
	return &http.Server{
		Addr: address, Handler: mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}
