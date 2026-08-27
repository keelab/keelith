package nacos

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/keelab/keelith/secret"
	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

// OpenConfigClient resolves credentials and constructs an owned official
// nacos config client.
func OpenConfigClient(
	ctx context.Context,
	config Config,
	resolver SecretResolver,
) (config_client.IConfigClient, error) {
	parameter, normalized, err := buildParameter(ctx, config, resolver)
	if err != nil {
		return nil, err
	}
	if err := prepareDirectories(normalized); err != nil {
		return nil, err
	}
	client, err := clients.NewConfigClient(parameter)
	if err != nil {
		return nil, fmt.Errorf("nacos runtime: create config client: %w", err)
	}
	return client, nil
}

// OpenNamingClient resolves credentials and constructs an owned official
// nacos naming client.
func OpenNamingClient(
	ctx context.Context,
	config Config,
	resolver SecretResolver,
) (naming_client.INamingClient, error) {
	parameter, normalized, err := buildParameter(ctx, config, resolver)
	if err != nil {
		return nil, err
	}
	if err := prepareDirectories(normalized); err != nil {
		return nil, err
	}
	client, err := clients.NewNamingClient(parameter)
	if err != nil {
		return nil, fmt.Errorf("nacos runtime: create naming client: %w", err)
	}
	return client, nil
}

func buildParameter(
	ctx context.Context,
	config Config,
	resolver SecretResolver,
) (vo.NacosClientParam, Config, error) {
	if ctx == nil {
		return vo.NacosClientParam{}, Config{}, invalidConfig(
			"context is nil",
		)
	}
	if cause := context.Cause(ctx); cause != nil {
		return vo.NacosClientParam{}, Config{}, cause
	}
	normalized, err := NormalizeConfig(config)
	if err != nil {
		return vo.NacosClientParam{}, Config{}, err
	}
	password, err := resolvePassword(
		ctx,
		normalized.PasswordReference,
		resolver,
	)
	if err != nil {
		return vo.NacosClientParam{}, Config{}, err
	}
	servers := make(
		[]constant.ServerConfig,
		0,
		len(normalized.Servers),
	)
	for _, server := range normalized.Servers {
		servers = append(servers, constant.ServerConfig{
			Scheme:      server.Scheme,
			ContextPath: server.ContextPath,
			IpAddr:      server.Address,
			Port:        server.Port,
			GrpcPort:    server.GRPCPort,
		})
	}
	client := &constant.ClientConfig{
		TimeoutMs:            uint64(normalized.Timeout.Milliseconds()),
		BeatInterval:         normalized.BeatInterval.Milliseconds(),
		NamespaceId:          normalized.Namespace,
		AppName:              normalized.AppName,
		CacheDir:             normalized.CacheDirectory,
		DisableUseSnapShot:   !normalized.UseSDKSnapshot,
		NotLoadCacheAtStart:  !normalized.UseSDKSnapshot,
		UpdateThreadNum:      normalized.UpdateThreads,
		UpdateCacheWhenEmpty: normalized.UpdateCacheEmpty,
		Username:             normalized.Username,
		Password:             password,
		LogDir:               normalized.LogDirectory,
		LogLevel:             normalized.LogLevel,
		AppendToStdout:       normalized.AppendLogToStdout,
		TLSCfg: constant.TLSConfig{
			Appointed:          true,
			Enable:             normalized.TLS.Enabled,
			TrustAll:           false,
			CaFile:             normalized.TLS.CAFile,
			CertFile:           normalized.TLS.CertFile,
			KeyFile:            normalized.TLS.KeyFile,
			ServerNameOverride: normalized.TLS.ServerName,
		},
	}
	return vo.NacosClientParam{
		ClientConfig:  client,
		ServerConfigs: servers,
	}, normalized, nil
}

func resolvePassword(
	ctx context.Context,
	rawReference string,
	resolver SecretResolver,
) (string, error) {
	if rawReference == "" {
		return "", nil
	}
	if resolver == nil {
		return "", invalidConfig(
			"secret resolver is required for passwordRef",
		)
	}
	reference, err := secret.Parse(rawReference)
	if err != nil {
		return "", invalidConfig("passwordRef: %v", err)
	}
	value, err := resolver.Resolve(ctx, reference)
	if err != nil {
		return "", fmt.Errorf("nacos runtime: resolve password: %w", err)
	}
	if err := value.Validate(); err != nil {
		return "", fmt.Errorf("nacos runtime: password value: %w", err)
	}
	if value.Expired(time.Now()) {
		return "", fmt.Errorf(
			"nacos runtime: password value: %w",
			secret.ErrInvalidValue,
		)
	}
	content := value.Bytes()
	defer clear(content)
	return string(secret.TrimLineBreaks(content)), nil
}

func prepareDirectories(config Config) error {
	for name, path := range map[string]string{
		"cache": config.CacheDirectory,
		"log":   config.LogDirectory,
	} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return fmt.Errorf(
				"nacos runtime: create %s directory: %w",
				name,
				err,
			)
		}
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf(
				"nacos runtime: stat %s directory: %w",
				name,
				err,
			)
		}
		if !info.IsDir() {
			return fmt.Errorf(
				"nacos runtime: %s directory: %w",
				name,
				errors.New("path is not a directory"),
			)
		}
	}
	return nil
}
