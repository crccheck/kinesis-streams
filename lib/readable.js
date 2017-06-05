// @flow weak
// http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Kinesis.html
const Readable = require('stream').Readable
const debug = require('debug')('kinesis-streams:readable')
const promiseRetry = require('promise-retry')


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
  /*:: maxRetries: number */
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
      interval: 2000,
      parser: (x) => x,
    }, options)
    this._started = 0  // TODO this is probably built into Streams
    this.iterators = new Set()
    this.maxRetries = 3
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
    // Not written using Promises because they make it harder to keep the program alive here
    this.retryAWS('getRecords', params, (err, data) => {
      if (err) {
        this.emit('error', err) || console.log(err, err.stack)
        return
      }

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
        // this._started = 0
        return
      }

      setTimeout(() => {
        this.readShard(data.NextShardIterator)
        // idleTimeBetweenReadsInMillis  http://docs.aws.amazon.com/streams/latest/dev/kinesis-low-latency.html
      }, this.options.interval)
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

  retryAWS (funcName/*: string */, params/*: Object */, cb)/*: Promise<Object> */ {
    return promiseRetry((retry/*: function */, number/*: number */) => {
      if (number > 1) {
        this.logger.warn('AWS %s.%s attempt: %d', this.client.constructor.__super__.serviceIdentifier, funcName, number)
      }
      return this.client[funcName](params).promise()
        .then((data) => {
          cb(null, data)
        })
        .catch((err) => {
          if (!err.retryable) {
            return cb(err)
          }

          retry(err)
        })
    }, {retries: this.maxRetries, minTimeout: this.options.interval})
  }
}


// EXPORTS
//////////

exports.getStreams = getStreams
exports.KinesisReadable = KinesisReadable
