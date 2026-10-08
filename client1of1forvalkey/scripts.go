package client1of1forvalkey

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

type operationKind uint8

const (
	operationAcquire operationKind = iota
	operationRenew
	operationRelease
)

const (
	acquireStatusBusy int64 = iota
	acquireStatusAcquired
	acquireStatusAlreadyOwned
	acquireStatusMemoryLimit
)

type operationResult struct {
	status int64
	ttlMS  uint64
	err    error
}

type scriptHashes struct {
	acquire string
	renew   string
	release string
}

const (
	decimalBase    = 10
	integerBitSize = 64
)

//nolint:dupword // Repeated "end" tokens are part of the embedded Lua syntax.
const acquireLua = `
local current = redis.call('GET', KEYS[1])
if not current then
    local ttl = tonumber(ARGV[2])
    if ttl > 0 then
        redis.call('PSETEX', KEYS[1], ttl, ARGV[1])
    end
    return {1, ttl}
end
if current == ARGV[1] then
    local ttl = redis.call('PTTL', KEYS[1])
    if ttl > 0 then
        return {2, ttl}
    end
end
return {0, 0}
`

const renewLua = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return {0, 0}
end
local current_ttl = redis.call('PTTL', KEYS[1])
if current_ttl <= 0 then
    return {0, 0}
end
local requested_ttl = tonumber(ARGV[2])
if requested_ttl > current_ttl then
    redis.call('PEXPIRE', KEYS[1], requested_ttl)
    current_ttl = requested_ttl
end
return {1, current_ttl}
`

const releaseLua = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
`

func (c *Client) execute(
	ctx context.Context,
	kind operationKind,
	valkeyKey string,
	token string,
	ttlMS uint64,
) (*redis.Cmd, uint64) {
	generation := c.scriptGeneration.Load()
	keys := []string{valkeyKey}
	switch kind {
	case operationAcquire:
		return c.pipeline.EvalSha(
			ctx,
			c.scripts.acquire,
			keys,
			token,
			strconv.FormatUint(ttlMS, decimalBase),
		), generation
	case operationRenew:
		return c.pipeline.EvalSha(
			ctx,
			c.scripts.renew,
			keys,
			token,
			strconv.FormatUint(ttlMS, decimalBase),
		), generation
	case operationRelease:
		return c.pipeline.EvalSha(ctx, c.scripts.release, keys, token), generation
	default:
		panic("client1of1forvalkey: unknown operation")
	}
}

func (c *Client) loadOperationResult(cmd *redis.Cmd, generation uint64) operationResult {
	status, ttlMS, err := decodeStatusAndTTL(cmd)
	if isNoScript(err) {
		_ = c.reloadScripts(c.ctx, generation)
	}
	return operationResult{status: status, ttlMS: ttlMS, err: err}
}

func (c *Client) reloadScripts(ctx context.Context, observedGeneration uint64) error {
	c.scriptMu.Lock()
	defer c.scriptMu.Unlock()
	if c.scriptGeneration.Load() != observedGeneration {
		return nil
	}

	operationCtx, cancel := context.WithTimeout(ctx, c.responseTimeout)
	defer cancel()
	_, err := c.redis.Pipelined(operationCtx, func(pipeline redis.Pipeliner) error {
		pipeline.ScriptLoad(operationCtx, acquireLua)
		pipeline.ScriptLoad(operationCtx, renewLua)
		pipeline.ScriptLoad(operationCtx, releaseLua)
		return nil
	})
	if err != nil {
		return err
	}
	c.scriptGeneration.Add(1)
	return nil
}

func decodeStatusAndTTL(cmd *redis.Cmd) (int64, uint64, error) {
	values, err := cmd.Slice()
	if err != nil {
		return 0, 0, err
	}
	if len(values) != 2 {
		return 0, 0, errors.New("valkey lease script returned an invalid response")
	}
	status, err := responseInt64(values[0])
	if err != nil {
		return 0, 0, err
	}
	ttl, err := responseInt64(values[1])
	if err != nil {
		return 0, 0, err
	}
	if ttl < 0 {
		return 0, 0, errors.New("valkey lease script returned a negative TTL")
	}
	return status, uint64(ttl), nil
}

func responseInt64(value any) (int64, error) {
	switch value := value.(type) {
	case int64:
		return value, nil
	case string:
		return strconv.ParseInt(value, decimalBase, integerBitSize)
	default:
		return 0, errors.New("valkey lease script returned a non-integer value")
	}
}

func isNoScript(err error) bool {
	return errors.Is(err, redis.ErrNoScript) || redis.HasErrorPrefix(err, "NOSCRIPT")
}

func isValkeyOOM(err error) bool {
	if err == nil {
		return false
	}
	return redis.HasErrorPrefix(err, "OOM") || strings.HasPrefix(strings.ToUpper(err.Error()), "OOM ")
}
