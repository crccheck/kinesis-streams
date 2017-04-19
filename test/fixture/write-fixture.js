'use strict'

var records = require('./records.json')

records = records.map(function (record) {
  return {
    Data: JSON.stringify(record),
    PartitionKey: '1234',
  }
})

module.exports = records
