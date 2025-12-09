'use strict'

const _ = require('lodash')
const async = require('async')
const Redis = require('ioredis')
const Base = require('@bitfinex/bfx-facs-base')

function client (conf, label) {
  const auth = conf.auth || conf.password

  delete conf.auth
  delete conf.password

  if (_.isString(auth) && auth !== '') {
    conf.password = auth
  }

  conf.keepAlive = 5000 // set keepalive with some delay

  const rc = new Redis(conf)

  rc.on('error', err => {
    console.error(new Date(), label || 'generic', err)
  })
  
  rc._pinger = setInterval(() => {
    if (rc.status !== 'ready') {
      return 
    }

    rc.ping()
  }, 15000)

  return rc
}

class RedisFacility extends Base {

  constructor (caller, opts, ctx) {
    super(caller, opts, ctx)

    this.name = 'redis'
    this._hasConf = true
    this._names = ['cli_rw', 'cli_sub']

    this.init()
  }

  onStream (channel, data, src, pattern) {
    let msg = null

    channel = channel.toString()
    
    if (this.caller.onRedisDataPrep0) {
      data = this.caller.onRedisDataPrep0(
        channel, data
      )
    }
    
    try {
      msg = JSON.parse(data)
    } catch (e) {
      console.error(e, data, channel, src, pattern)
      msg = null
    }

    if (!msg) {
      return
    }

    this.emit('message', channel, msg, src, pattern)
  }

  _start (cb) {
    async.series([
      next => { super._start(next) },
      next => {
        _.each(this._names, pfx => {
          this[pfx] = client(_.pick(this.conf, ['host', 'port', 'auth', 'sentinels', 'name']))
        })

        if (this.cli_sub) {
          const subSfx = this.opts.subBuffer ? 'Buffer' : ''

          this.cli_sub.on(`message${subSfx}`, (channel, data) => {
            this.onStream(channel, data, this.opts.ns + '_sub')
          })

          this.cli_sub.on(`pmessage${subSfx}`, (pattern, channel, data) => {
            this.onStream(channel, data, this.opts.ns + '_sub', pattern)
          })
        }

        next()
      }
    ], cb)
  }

  _stop (cb) {
    async.series([
      next => { super._stop(next) },
      next => {
        _.each(this._names, pfx => {
          clearInterval(this[pfx]._pinger)
          this[pfx].removeAllListeners('message')
          this[pfx].removeAllListeners('pmessage')
          this[pfx].disconnect()
          delete this[pfx]
        })
        next()
      }
    ], cb)
  }

  _scriptPoll () {
    return {
      numberOfKeys: 1,
      lua: "local len = redis.call('llen', KEYS[1]) \
        if len == 0 then \
         return nil \
      end \
      local result = redis.call('lrange', KEYS[1], 0, ARGV[1] - 1) \
      redis.call('ltrim', KEYS[1], ARGV[1], -1) \
      return result"
    }
  }
}

module.exports = RedisFacility
