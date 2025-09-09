'use strict'

const crypto = require('crypto')
const { GrcUserError, GrcGenericError } = require('@bitfinex/lib-util-err-js')

const RATE_LIMIT_LUA = `
    local attempts = redis.call('INCR', KEYS[1])
    if attempts == 1 then
      redis.call('EXPIRE', KEYS[1], ARGV[1])
    end
    return attempts
  `

class RedisRateLimiterUtil {
  static register ({ redis, command, keyPrefix, logError }) {
    return new RedisRateLimiterUtil({ redis, command, keyPrefix, logError })._register()
  }

  constructor ({ redis, command, keyPrefix, logError }) {
    this._redis = redis
    this._command = command
    this._keyPrefix = keyPrefix
    this._logError = logError
    this._registered = false
  }

  async checkRateLimit (key, expiry, maxAttempts) {
    if (!this._registered) return this._logError(new GrcGenericError('RedisRateLimiter not registered'))
    const attempts = await this._redis[this._command](`${this._keyPrefix}:${key}`, expiry)
    if (attempts > maxAttempts) {
      throw new GrcUserError('ERR_RATE_LIMIT_EXCEEDED')
    }
  }

  _register () {
    const md5 = crypto.createHash('md5').update(RATE_LIMIT_LUA, 'utf8').digest('hex')
    const commandSymbol = Symbol.for(this._command)
    if (this._redis[this._command] || this._redis[commandSymbol] === md5) return this
    try {
      this._redis.defineCommand(this._command, {
        numberOfKeys: 1,
        lua: RATE_LIMIT_LUA
      })
      this._redis[commandSymbol] = md5
      this._registered = true
    } catch (error) {
      this._logError(error)
    }
    return this
  }
}

module.exports = {
  RedisRateLimiterUtil
}
