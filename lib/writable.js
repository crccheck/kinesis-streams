// @flow
const debug = require('debug')('kinesis-streams:writable')
const FlushWritable = require('flushwritable')

const TOO_MANY_RECORDS = '%d is too high; there is a hard limit of 500 records in one batch. See https://docs.aws.amazon.com/firehose/latest/APIReference/API_PutRecordBatch.html'
/*::
type OriginalData = Object | Array<any>
type Record = {Data: string, PartitionKey: string}
type AWSResponseRecord = {SequenceNumber: string, ShardId: string} |
  {ErrorCode: string, ErrorMessage: string}
type AWSResponse = {
  FailedRecordCount: number,
  Records: AWSResponseRecord[],
}
type ConsoleLogger = {
  debug: Function, info: Function, warn: Function
}
type Options = {
  highWaterMark?: number,
  logger?: ConsoleLogger,
  wait?: number,
}
*/

class KinesisWritable extends FlushWritable {
  /*::
    _queueCheckTimer: ?TimeoutID
    client: Object
    logger: ConsoleLogger
    streamName: string
    queue: Record[]
    collectionMaxCount: number
    collectionMaxSize: number
    recordMaxBufferedTime: number
  */
  constructor (client/*: Object */, streamName/*: string */, { highWaterMark = 16, logger, wait = 500 }/*: Options */ = {}) {
    if (!client) {
      throw new Error('client is required')
    }
    if (!streamName) {
      throw new Error('streamName is required')
    }

    super({ objectMode: true, highWaterMark: Math.min(highWaterMark, 500) })

    this._queueCheckTimer = null
    this.client = client
    this.streamName = streamName
    this.logger = logger || { debug: debug, info: debug, warn: debug }
    if (highWaterMark > 500) {
      this.logger.warn(TOO_MANY_RECORDS, highWaterMark)
      highWaterMark = 500
    }
    this.queue = []

    // Kinesis options
    this.collectionMaxCount = highWaterMark
    // ASSERT this.collectionMaxCount === this._writableState.highWaterMark
    this.collectionMaxSize = 4194304 // TODO make this an option
    this.recordMaxBufferedTime = wait
  }

  _write (data/*: OriginalData */, encoding/*: any */, callback/*: Function */) {
    this.logger.debug('Adding to Kinesis queue', data)
    this.queue.push(this._prepRecord(data))
    const queueSize = this.queue.reduce((a, b) => b.Data.length + a, 0)
    if (this.queue.length >= this.collectionMaxCount || queueSize >= this.collectionMaxSize) {
      return this.writeRecords()
        .then(callback)
        .catch(callback)
    }

    if (this._queueCheckTimer) {
      clearTimeout(this._queueCheckTimer)
    }
    this._queueCheckTimer = setTimeout(() => this.writeRecords(), this.recordMaxBufferedTime)

    callback()
  }

  _flush (callback/*: Function */) {
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

  getPartitionKey (data/*: OriginalData */)/*: string */ {
    return '0'
  };

  /**
   * Count the number of elements from the beginning of this.queue that will
   * meet all limitations
   */
  getQueueSpliceCount ()/*: number */ {
    let collectionSize = 0
    const maxCount = Math.min(this.queue.length, this.collectionMaxCount)
    let idx
    for (idx = 0; idx < maxCount; idx++) {
      collectionSize += this.queue[idx].Data.length
      if (collectionSize >= this.collectionMaxSize) {
        return idx + 1
      }
    }

    return maxCount
  }

  _prepRecord (data/*: OriginalData */)/*: Record */ {
    return {
      Data: JSON.stringify(data), // TODO let user override this
      PartitionKey: this.getPartitionKey(data),
    }
  };

  async writeRecords ()/*: Promise<void> */ {
    if (!this.queue.length) {
      return
    }

    const records = this.queue.splice(0, this.getQueueSpliceCount())
    this.logger.debug('Writing %d records to Kinesis', records.length)

    let response
    try {
      response = await this.client.putRecords({
        Records: records,
        StreamName: this.streamName,
      }).promise()
    } catch (err) {
      this.queue.unshift(...records)
      throw err
    }

    // ASSERT response.Records.length === records.length
    this.logger.info('Wrote %d records to Kinesis', records.length - response.FailedRecordCount)

    if (response.FailedRecordCount !== 0) {
      this.logger.warn('Failed %d records to Kinesis', response.FailedRecordCount)

      const failedRecords = []
      response.Records.forEach((record, idx) => {
        if (record.ErrorCode) {
          // $FlowFixMe Why can't Flow guess based on the `if`?
          this.logger.warn('Failed record with message: %s', record.ErrorMessage)
          failedRecords.push(records[idx])
        }
      })
      this.queue.unshift(...failedRecords)
    }

    this.emit('kinesis.putRecords', response)
  }
}

exports.KinesisWritable = KinesisWritable
