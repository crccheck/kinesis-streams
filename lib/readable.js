// @flow weak
// http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Kinesis.html
const Readable = require('stream').Readable
const debug = require('debug')('kinesis-streams:readable')

function sleep (timeout, ...args) {
  if (timeout === 0) {
    return Promise.resolve(...args)
  }

  return new Promise((resolve) => {
    setTimeout(() => resolve(...args), timeout)
  })
}

function getStreams (client) {
  return client.listStreams({}).promise()
}

class KinesisReadable extends Readable {
  /*:: client: Object */
  /*:: logger: {debug: Function, info: Function, warn: Function} */
  /*:: options: Object */
  /*:: streamName: string */
  /*:: _started: 0|1|2 */
  /*:: iterators: Set<string> */
  constructor (client/*: Object */, streamName/*: string */, options = {}) {
    if (!client) {
      throw new Error('client is required')
    }
    if (!streamName) {
      throw new Error('streamName is required')
    }

    super({objectMode: true})

    this.client = client
    this.streamName = streamName
    this.logger = options.logger || {debug: debug, info: debug, warn: debug}
    this.options = Object.assign({
      // idleTimeBetweenReadsInMillis  http://docs.aws.amazon.com/streams/latest/dev/kinesis-low-latency.html
      interval: 2000,
      parser: (x) => x,
    }, options)
    this._started = 0  // TODO this is probably built into Streams
    this.iterators = new Set()
  }

  getShardId () {
    const params = {
      StreamName: this.streamName,
    }
    return this.client.describeStream(params).promise()
      .then((data) => {
        if (!data.StreamDescription.Shards.length) {
          throw new Error('No shards!')
        }

        this.logger.info('getShardId found %d shards', data.StreamDescription.Shards.length)
        return data.StreamDescription.Shards.map((x) => x.ShardId)
      })
  }

  getShardIterator (shardId/*: string */, options/*: Object */) {
    const params = Object.assign({
      ShardId: shardId,
      ShardIteratorType: 'LATEST',
      StreamName: this.streamName,
    }, options || {})
    return this.client.getShardIterator(params).promise()
      .then((data) => {
        this.logger.info('getShardIterator got iterator id: %s', data.ShardIterator)
        return data.ShardIterator
      })
  }

  _startKinesis () {
    const whitelist = ['ShardIteratorType', 'Timestamp', 'StartingSequenceNumber']
    const shardIteratorOptions = Object.keys(this.options)
      .filter((x) => whitelist.indexOf(x) !== -1)
      .reduce((result, key) => Object.assign(result, {[key]: this.options[key]}), {})
    return this.getShardId()
      .then((shardIds) => {
        const shardIterators = shardIds.map((shardId) =>
          this.getShardIterator(shardId, shardIteratorOptions))
        return Promise.all(shardIterators)
      })
      .then((shardIterators) => {
        shardIterators.forEach((shardIterator) => this.readShard(shardIterator))
      })
      .catch((err) => {
        this.emit('error', err) || console.log(err, err.stack)
      })
  }

  readShard (shardIterator/*: string */) {
    this.iterators.add(shardIterator)
    this.logger.info('readShard starting from %s (out of %d)', shardIterator, this.iterators.size)
    const params = {
      ShardIterator: shardIterator,
      Limit: 10000,  // https://github.com/awslabs/amazon-kinesis-client/issues/4#issuecomment-56859367
    }
    // This will be a lot cleaner with async/await
    return this.client.getRecords(params).promise()
      .then((data) => {
        if (data.MillisBehindLatest > 60 * 1000) {
          this.logger.warn('behind by %d milliseconds', data.MillisBehindLatest)
        }
        data.Records.forEach((x) => this.push(this.options.parser(x.Data)))
        if (data.Records.length) {
          this.emit('checkpoint', data.Records[data.Records.length - 1].SequenceNumber)
        }
        this.iterators.delete(shardIterator)
        if (!data.NextShardIterator) {
          this.logger.info('readShard.closed %s', shardIterator)
          // TODO this.end() when number of shards closed == number of shards being read
          return null
        }

        return data.NextShardIterator
      })
      .then((nextShardIterator) => {
        if (nextShardIterator) {
          return sleep(this.options.interval, nextShardIterator)
        }

        return null
      })
      .then((nextShardIterator) => {
        if (nextShardIterator) {
          return this.readShard(nextShardIterator)
        }

        return null
      })
      .catch((err) => {
        this.emit('error', err)
        return null
      })
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
