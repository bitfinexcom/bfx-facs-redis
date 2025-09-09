/* eslint-env mocha */

'use strict'

const chai = require('chai')
const chaiAsPromised = require('chai-as-promised')
chai.use(chaiAsPromised)

const { expect } = require('chai')
const RedisFacility = require('../../index')
const { RedisRateLimiterUtil } = require('../../libs/redis.rate.limiter')

describe('Redis Rate Limiter Integration', () => {
  let redisFacility, redis, redisRateLimiter

  before(async () => {
    redisFacility = new RedisFacility({ ctx: { root: './' } }, {}, {})
    await new Promise(resolve => redisFacility.start(() => resolve()))
    redis = redisFacility.cli_rw
  })

  after(async () => {
    await new Promise(resolve => redisFacility.stop(() => resolve()))
  })

  beforeEach(async () => {
    await redis.flushdb()
    redisRateLimiter = RedisRateLimiterUtil.register({
      redis,
      command: 'bfxRateLimit',
      keyPrefix: 'bfx',
      logError: console.error
    })
  })

  it('should check rate limit', async () => {
    await redisRateLimiter.checkRateLimit('some-key', 1, 1)
    await expect(redisRateLimiter.checkRateLimit('some-key', 1, 1))
      .to.be.rejectedWith('ERR_RATE_LIMIT_EXCEEDED')
  })

  it('should reset rate limit after some time', async () => {
    await redisRateLimiter.checkRateLimit('another-key', 1, 1)
    await sleep(1100)
    await redisRateLimiter.checkRateLimit('another-key', 1, 1)
    expect(true).to.deep.eq(true)
  })

  const sleep = (ms) => new Promise((resolve) => {
    setTimeout(() => resolve(), ms)
  })
})
