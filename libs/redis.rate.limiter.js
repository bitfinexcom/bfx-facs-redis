'use strict'

const RATE_LIMIT_LUA = `
    local attempts = redis.call('INCR', KEYS[1])
    if attempts == 1 then
      redis.call('EXPIRE', KEYS[1], ARGV[1])
    end
    return attempts
  `

class RedisRateLimiter {
  constructor ({ redisFac, command, keyPrefix }) {
    this._redis = redisFac.cli_rw
    this._command = command ?? 'rate_limit'
    redisFac.defineCommand(this._command, RATE_LIMIT_LUA, 1)
    this._keyPrefix = keyPrefix ? `${keyPrefix}:` : ''
  }

  async checkRateLimit (key, expiry, maxAttempts) {
    const attempts = await this._redis[this._command](`${this._keyPrefix}${key}`, expiry)
    if (attempts > maxAttempts) {
      throw new Error('ERR_RATE_LIMIT_EXCEEDED')
    }
  }
}

module.exports = {
  RedisRateLimiter
}
