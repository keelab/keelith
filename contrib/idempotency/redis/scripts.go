package redis

const claimScript = `
local state = redis.call('HGET', KEYS[1], 'state')
if not state then
  redis.call('HSET', KEYS[1],
    'state', 'processing',
    'fingerprint', ARGV[1],
    'owner', ARGV[2])
  redis.call('PEXPIRE', KEYS[1], ARGV[3])
  return {0, '', 0}
end

local fingerprint = redis.call('HGET', KEYS[1], 'fingerprint')
if fingerprint ~= ARGV[1] then
  return {3, '', 0}
end

if state == 'processing' then
  local ttl = redis.call('Pttl', KEYS[1])
  if ttl < 0 then ttl = 0 end
  return {1, '', ttl}
end

if state == 'completed' then
  local result = redis.call('HGET', KEYS[1], 'result')
  if not result then result = '' end
  return {2, result, 0}
end

return {-1, '', 0}
`

const completeScript = `
if redis.call('HGET', KEYS[1], 'state') ~= 'processing' then
  return 0
end
if redis.call('HGET', KEYS[1], 'fingerprint') ~= ARGV[1] then
  return 0
end
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[2] then
  return 0
end
redis.call('HSET', KEYS[1], 'state', 'completed', 'result', ARGV[3])
redis.call('HDEL', KEYS[1], 'owner')
redis.call('PEXPIRE', KEYS[1], ARGV[4])
return 1
`

const abandonScript = `
if redis.call('HGET', KEYS[1], 'state') ~= 'processing' then
  return 0
end
if redis.call('HGET', KEYS[1], 'fingerprint') ~= ARGV[1] then
  return 0
end
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[2] then
  return 0
end
redis.call('DEL', KEYS[1])
return 1
`
