// @flow weak
const debug = require('debug')('kinesis-streams:writable')
const promiseRetry = require('promise-retry')
const FlushWritable = require('flushwritable')

const TOO_MANY_RECORDS = '%d is too high; there is a hard limit of 500 records in one batch. See https://docs.aws.amazon.com/firehose/latest/APIReference/API_PutRecordBatch.html'
/*:: type Record = {Data: string, PartitionKey: string} */

class KinesisWritable extends FlushWritable {
  /*:: client: Object */
  /*:: logger: {debug: Function, info: Function, warn: Function} */
  /*:: queue: Object[] */
  /*:: streamName: string */
  /*:: highWaterMark: number */
  /*:: wait: number */
  /*:: _queueCheckTimer: number */
  constructor (client, streamName, {highWaterMark = 16, logger, maxRetries = 3, retryTimeout = 100, wait = 500} = {}) {
    if (!client) {
      throw new Error('client is required')
    }
    if (!streamName) {
      throw new Error('streamName is required')
    }

    super({objectMode: true, highWaterMark: Math.min(highWaterMark, 500)})

    this.client = client
    this.streamName = streamName
    this.logger = logger || {debug: debug, info: debug, warn: debug}
    if (highWaterMark > 500) {
      this.logger.warn(TOO_MANY_RECORDS, highWaterMark)
      highWaterMark = 500
    }
    this.highWaterMark = highWaterMark
    // assert.equal(this.highWaterMark, this._writableState.highWaterMark)
    this.maxRetries = 3
    this.retryTimeout = retryTimeout
    this.wait = wait
    this._queueCheckTimer = setTimeout(() => this.writeRecords(), this.wait)

    this.queue = []
  }

  _write (data, encoding, callback) {
    this.logger.debug('Adding to Kinesis queue', data)

    this.queue.push(data)

    if (this.queue.length >= this.highWaterMark) {
      return this.writeRecords()
        .then(callback)
        .catch(callback)
    }

    if (this._queueCheckTimer) {
      clearTimeout(this._queueCheckTimer)
    }
    this._queueCheckTimer = setTimeout(() => this.writeRecords(), this.wait)

    callback()
  }

  _flush (callback) {
    if (this._queueCheckTimer) {
      clearTimeout(this._queueCheckTimer)
    }

    return this.writeRecords()
      .then(() => {
        if (this.queue.length) {
          return this._flush(() => null)
        }
      })
      .then(callback)
      .catch(callback)
  }

  // eslint-disable-next-line no-unused-vars
  getPartitionKey (data/*: Object */)/*: string */ {
    return '0'
  };

  _prepRecord (data/*: Object */)/*: Record */ {
    return {
      Data: JSON.stringify(data),
      PartitionKey: this.getPartitionKey(data),
    }
  };

  writeRecords ()/*: Promise<void> */ {
    return new Promise((resolve, reject) => {
      if (!this.queue.length) {
        return resolve()
      }

      this.logger.debug('Writing %d records to Kinesis', this.queue.length)
      const dataToPut = this.queue.splice(0, Math.min(this.queue.length, this.highWaterMark))
      const records = dataToPut.map(this._prepRecord.bind(this))

      this.retryAWS('putRecords', {
        Records: records,
        StreamName: this.streamName,
      })
        .then((response) => {
          // assert records.length === response.Records.length
          this.logger.info('Wrote %d records to Kinesis', records.length - response.FailedRecordCount)

          if (response.FailedRecordCount !== 0) {
            this.logger.warn('Failed %d records to Kinesis', response.FailedRecordCount)

            const failedRecords = []
            response.Records.forEach((record, idx) => {
              if (record.ErrorCode) {
                this.logger.warn('Failed record with message: %s', record.ErrorMessage)
                failedRecords.push(dataToPut[idx])
              }
            })
            this.queue.unshift(...failedRecords)
          }

          this.emit('kinesis.putRecords', response)
          return resolve()
        })
        .catch((err) => {
          this.queue.unshift(...dataToPut)
          reject(err)
        })
    })
  }

  retryAWS (funcName/*: string */, params/*: Object */)/*: Promise<Object> */ {
    return promiseRetry((retry/*: function */, number/*: number */) => {
      if (number > 1) {
        this.logger.warn('AWS %s.%s attempt: %d', this.client.constructor.__super__.serviceIdentifier, funcName, number)
      }
      return this.client[funcName](params).promise()
        .catch((err) => {
          if (err.retryable === false) {
            throw err // _write will catch this and pass it back to `callback`
          }

          retry(err)
        })
    }, {retries: this.maxRetries, minTimeout: this.retryTimeout})
  }
}

exports.KinesisWritable = KinesisWritable
