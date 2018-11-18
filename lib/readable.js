// @flow
// http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Kinesis.html
const { Readable } = require('stream')
const debug = require('debug')('kinesis-streams:readable')

function sleep (timeout/*: number */)/*: ?Promise<any> */ {
  if (timeout === 0) {
    return
  }

  return new Promise((resolve) => {
    setTimeout(() => resolve(), timeout)
  })
}

function getStreams (client/*: Object */)/*: Promise<Object> */ {
  return client.listStreams({}).promise()
}

class KinesisReadable extends Readable {
  /*::
  client: Object
  logger: {debug: Function, info: Function, warn: Function}
  options: Object
  streamName: string
  restartOnClose: boolean
  _started: 0|1|2
  iterators: Set<string>
  _readableState: Object
  */
  constructor (client/*: Object */, streamName/*: string */, options/*: Object */ = {}) {
    if (!client) {
      throw new Error('client is required')
    }
    if (!streamName) {
      throw new Error('streamName is required')
    }

    super({ objectMode: true })

    this.client = client
    this.streamName = streamName
    this.logger = options.logger || { debug, info: debug, warn: debug }
    this.options = Object.assign({
      // idleTimeBetweenReadsInMillis  http://docs.aws.amazon.com/streams/latest/dev/kinesis-low-latency.html
      interval: 2000,
      parser: (x) => x,
      // if true then gets a new list of shards once all have been closed (support for scaling streams)
      restartOnClose: false,
    }, options)
    this._started = 0 // TODO this is probably built into Streams
    this.iterators = new Set()
  }

  async getShardId () {
    const params = {
      StreamName: this.streamName,
    }
    const data = await this.client.describeStream(params).promise()
    if (!data.StreamDescription.Shards.length) {
      throw new Error('No shards!') // _startKinesis will catch this and emit the error
    }

    this.logger.info('getShardId found %d shards', data.StreamDescription.Shards.length)
    return data.StreamDescription.Shards.map((x) => x.ShardId)
  }

  async getShardIterator (shardId/*: string */, options/*: Object */ = {}) {
    const params = {
      ShardId: shardId,
      ShardIteratorType: 'LATEST',
      StreamName: this.streamName,
      ...options,
    }
    const data = await this.client.getShardIterator(params).promise()
    this.logger.info('getShardIterator got iterator id: %s', data.ShardIterator)
    return data.ShardIterator
  }

  async _startKinesis ()/*: Promise<void> */ {
    const whitelist = ['ShardIteratorType', 'Timestamp', 'StartingSequenceNumber']
    const shardIteratorOptions = Object.keys(this.options)
      .filter((x) => whitelist.includes(x))
      .reduce((result, key) => Object.assign(result, { [key]: this.options[key] }), {})
    try {
      const shardIds = await this.getShardId()
      const shardIterators = await Promise.all(shardIds.map((shardId) =>
        this.getShardIterator(shardId, shardIteratorOptions)))
      shardIterators.forEach((shardIterator) => this.readShard(shardIterator))
    } catch (err) {
      this.emit('error', err) || console.log(err, err.stack)
    }
  }

  async readShard (shardIterator/*: string */) {
    this.iterators.add(shardIterator)
    this.logger.info('readShard starting from %s (out of %d)', shardIterator, this.iterators.size)
    const params = {
      ShardIterator: shardIterator,
      Limit: 10000, // https://github.com/awslabs/amazon-kinesis-client/issues/4#issuecomment-56859367
    }
    let keepGoing = true
    let data
    try {
      data = await this.client.getRecords(params).promise()
    } catch (err) {
      this.emit('error', err)
      // if the error says it can be retried then keep going
      keepGoing = err.retryable
      data = null
    }

    this.iterators.delete(shardIterator)

    let nextShardIterator = shardIterator
    if (data) {
      if (data.MillisBehindLatest > 60 * 1000) {
        this.logger.warn('readShard behind by %d milliseconds', data.MillisBehindLatest)
      }
      data.Records.forEach((x) => this.push(this.options.parser(x.Data)))
      if (data.Records.length) {
        this.emit('checkpoint', data.Records[data.Records.length - 1].SequenceNumber)
      }
      if (!data.NextShardIterator) {
        this.logger.info('readShard.closed (%d remaining): %s', this.iterators.size, shardIterator)
        // TODO this.end() when number of shards closed == number of shards being read
        // if no more iterators left then restart the process again and get new shards
        if (this.iterators.size === 0 && this.options.restartOnClose) {
          this.logger.info('All shards closed, checking for new shards')
          return this._startKinesis()
        }
        return null
      }
      nextShardIterator = data.NextShardIterator
    }

    if (keepGoing) {
      // make sure we know that we are still watching a shard
      this.iterators.add(shardIterator)
      await sleep(this.options.interval)
      return this.readShard(nextShardIterator)
    }
  }

  _read (size/*: number|void */) {
    if (this._started) {
      return
    }

    this._startKinesis()
      .then(() => {
        this._started = 2
      })
      .catch((err) => {
        this.emit('error', err) || console.log(err, err.stack)
      })
    this._started = 1
  }
}


// EXPORTS
//////////

exports.getStreams = getStreams
exports.KinesisReadable = KinesisReadable
