/* eslint no-console: 0 */
const DBWrkrMongoDb = require('../dbwrkr-mongodb');
const tests = require('dbwrkr').tests;

tests({
  storage: new DBWrkrMongoDb({
    dbName: 'dbwrkr_tests'
  })
});
