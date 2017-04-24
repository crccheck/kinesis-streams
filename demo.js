const { Readable } = require('stream')
const AWS = require('aws-sdk')
const bunyan = require('bunyan')
const { KinesisWritable } = require('kinesis-streams')

const WAIT = 500

const logger = bunyan.createLogger({name: 'demo', level: 'debug'})

class NoiseReadable extends Readable {
  constructor (options = {}) {
    options.objectMode = true
    super(options)
    this._alphabet = '0123456789ABCDEF'.split('')
  }

  _read (size) {
    const throbber = this._alphabet.shift()
    const data = throbber
    // const data = {foo: throbber}
    this._alphabet.push(throbber)

    setTimeout(() => this.push(data), WAIT * 1.1 * Math.random())
  }
}

const client = new AWS.Kinesis()
const stream = new KinesisWritable(client, process.argv[2] || 'demo', {logger, wait: WAIT})

new NoiseReadable().pipe(stream)
