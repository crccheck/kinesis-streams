// Usage: node demo.js [streamName]
//
// Sends a trickle of data into a Kinesis stream using KinesisWritable.
// Designed with some jitter so sometimes you'll hit the highWaterMark and
// sometimes you'll hit the timeout.
//
// If you want prettier logging:
//
//     node demo.js hello-world | bunyan --output short
//
// And to verify the stream output:
//
// kinesis-console-consumer hello-world
//
// In local dev, if you get "Cannot find module 'kinesis-streams'", run `npm link`
//
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

stream.on('kinesis.putRecords', (response) => {
  /* eslint-disable no-unused-vars */
  const failedCount = response.FailedRecordCount
  const succcessCount = response.Records.length - response.FailedRecordCount
  const queueDepth = stream.queue.length
  /* eslint-enable */
})
stream.on('error', () => {
  /* eslint-disable no-unused-vars */
  const errorCount = 1
  const queueDepth = stream.queue.length
  /* eslint-enable */
})

new NoiseReadable().pipe(stream)
