'use strict'

const _ = require('lodash')
const async = require('async')
const Redis = require('ioredis')
const Base = require('bfx-facs-base')

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
    console.error(label || 'generic', err)
  })

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
          this.cli_sub.on('message', (channel, data) => {
            this.onStream(channel, data, this.opts.ns + '_sub')
          })

          this.cli_sub.on('pmessage', (pattern, channel, data) => {
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
          this[pfx].removeAllListeners('message')
          this[pfx].removeAllListeners('pmessage')
          this[pfx].disconnect()
          delete this[pfx]
        })
        next()
      }
    ], cb)
  }
}

module.exports = RedisFacility
