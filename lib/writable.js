// @flow weak
const promiseRetry = require('promise-retry')
const FlushWritable = require('flushwritable')

/*:: type Record = {Data: string, PartitionKey: string} */

class KinesisWritable extends FlushWritable {
  /*:: client: Object */
  /*:: logger: {debug: Function, info: Function, warn: Function} */
  /*:: queue: Object[] */
  /*:: streamName: string */
  /*:: highWaterMark: number */
  /*:: wait: number */
  /*:: _queueCheckTimer: number */
  constructor (client, streamName, {highWaterMark = 16,
                                    logger,
                                    maxRetries = 3,
                                    retryTimeout = 100,
                                    wait = 500} = {}) {
    super({objectMode: true, highWaterMark: Math.min(highWaterMark, 500)})

    if (!client) {
      throw new Error('client is required')
    }
    this.client = client

    if (!streamName) {
      throw new Error('streamName is required')
    }
    this.streamName = streamName
    this.logger = logger || {debug: () => null, info: () => null, warn: () => null}
    if (highWaterMark > 500) {
      this.logger.warn('The maximum number of records that can be added is 500, not %d', highWaterMark)
      highWaterMark = 500
    }
    this.highWaterMark = highWaterMark
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
        this.logger.info('Wrote %d records to Kinesis', records.length - response.FailedRecordCount)
        this.emit('kinesis.putRecords', response)

        if (response.FailedRecordCount !== 0) {
          this.logger.warn('Failed writing %d records to Kinesis', response.FailedRecordCount)

          const failedRecords = []
          response.Records.forEach((record, idx) => {
            if (record.ErrorCode) {
              this.logger.warn('Failed record with message: %s', record.ErrorMessage)
              failedRecords.push(dataToPut[idx])
            }
          })
          this.queue.unshift(...failedRecords)

          return resolve()
        }

        return resolve()
      })
      .catch((err) => reject(err))
    })
  }

  retryAWS (funcName/*: string */, params/*: Object */)/*: Promise<Object> */ {
    return promiseRetry((retry/*: function */, number/*: number */) => {
      if (number > 1) {
        this.logger.warn('AWS %s.%s attempt: %d', this.client.constructor.__super__.serviceIdentifier, funcName, number)
      }
      return this.client[funcName](params).promise()
        .catch((err) => {
          if (!err.retryable) {
            throw new Error(err)
          }

          retry(err)
        })
    }, {retries: this.maxRetries, minTimeout: this.retryTimeout})
  }
}

module.exports = KinesisWritable
