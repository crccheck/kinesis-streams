const readable = require('./readable')
const writable = require('./writable')

exports.getStreams = readable.getStreams
exports.KinesisReadable = readable.KinesisReadable
exports.KinesisWritable = writable.KinesisWritable
