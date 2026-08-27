package redis

import (
	"context"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

const tokenBucketLua = `
local now = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local capacity = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])

local state = redis.call("HMGET", KEYS[1], "tokens", "ts")
local tokens = capacity
local last = now
if state[1] and state[2] then
    tokens = tonumber(state[1])
    last = tonumber(state[2])
    if last > now then
        last = now
    end
    local elapsed = now - last
    tokens = math.min(
        capacity,
        tokens + math.floor(elapsed * rate / 1000000)
    )
end

local allowed = 0
local retry = 0
if tokens >= cost then
    tokens = tokens - cost
    allowed = 1
else
    retry = math.ceil((cost - tokens) * 1000000 / rate)
end

redis.call("HSET", KEYS[1], "tokens", tokens, "ts", now)
redis.call("PEXPIRE", KEYS[1], ttl)
return {allowed, tokens, retry}
`

var tokenBucketScript = goredis.NewScript(tokenBucketLua)

type goRedisBackend struct {
	client goredis.UniversalClient
}

func (backend *goRedisBackend) Ping(ctx context.Context) error {
	return backend.client.Ping(ctx).Err()
}

func (backend *goRedisBackend) Time(
	ctx context.Context,
) (time.Time, error) {
	return backend.client.Time(ctx).Result()
}

func (backend *goRedisBackend) RunTokenBucket(
	ctx context.Context,
	key string,
	args ...int64,
) (any, error) {
	values := make([]any, len(args))
	for index, value := range args {
		values[index] = value
	}
	return tokenBucketScript.Run(
		ctx,
		backend.client,
		[]string{key},
		values...,
	).Result()
}

func (backend *goRedisBackend) Close() error {
	return backend.client.Close()
}
