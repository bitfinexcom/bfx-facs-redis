'use strict'

const test = require('brittle')
const RedisFacility = require('../../index')
const { RedisRateLimiter } = require('../../libs/redis.rate.limiter')

let redisFacility, redis, redisRateLimiter

const redisTestSuite = (testSuite) => async (t) => {
  redisFacility = new RedisFacility({ ctx: { root: './' } }, {}, {})
  await new Promise(resolve => redisFacility.start(() => resolve()))
  redis = redisFacility.cli_rw
  redisRateLimiter = new RedisRateLimiter({
    redisFac: redisFacility,
    keyPrefix: 'bfx'
  })

  await testSuite(t)

  await new Promise(resolve => redisFacility.stop(() => resolve()))
}

const redisTest = (testBlock) => async (t) => {
  await redis.flushdb()

  await testBlock(t)
}

test('Redis Rate Limiter Integration', redisTestSuite(async (t) => {
  await t.test('should check rate limit', redisTest(async (t) => {
    await redisRateLimiter.checkRateLimit('some-key', 1, 1)
    await t.exception(redisRateLimiter.checkRateLimit('some-key', 1, 1), /^Error: ERR_RATE_LIMIT_EXCEEDED$/)
  }))

  await t.test('should reset rate limit after some time', redisTest(async (t) => {
    await redisRateLimiter.checkRateLimit('another-key', 1, 1)
    await sleep(1100)
    await redisRateLimiter.checkRateLimit('another-key', 1, 1)
    t.pass()
  }))
}))

const sleep = (ms) => new Promise((resolve) => {
  setTimeout(() => resolve(), ms)
})
