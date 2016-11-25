var util = require('util');

var assert = require('assert');
var debug = require('debug')('dbwrkr:mongodb');
var mongo = require('mongodb');

var mongoClient = mongo.MongoClient;


// Document schema
//
// subscripions:
//  eventName   String  (indexed)
//  queues      [String]
//
// qitems:
//  name        String  (indexed)
//  queue       String  (indexed)
//  tid         String
//  payload     Object
//  parent      _ObjectId
//  created     Date
//  when        Date    (sparse indexed)
//  done        Date    (sparse indexed)
//  retryCount  Number


function DbWrkrMongoDB(opt) {
  this.host = opt.host || 'localhost';
  this.port = opt.port || 27017;
  this.dbName = opt.dbName;

  assert(this.dbName, 'has database name');
  this.db = null;
  this.dbSubscriptions = null;
  this.dbQitems = null;
}


DbWrkrMongoDB.prototype.connect = function connect(done) {
  var self = this;

  var cnStr = util.format('mongodb://%s:%s/%s',
    this.host,
    this.port,
    this.dbName
  );

  // var cnStr = `mongodb://${this.host}:${this.port}/${this.dbName}`;
  mongoClient.connect(cnStr, {w: 1}, function (err, db) {
    if (err) return done(err);

    self.db = db;
    self.dbSubscriptions = self.db.collection('wrkr_subscriptions');
    self.dbQitems = self.db.collection('wrkr_qitems');

    self.dbSubscriptions.createIndex({event: 1}, checkIndexResult);
    self.dbQitems.createIndex({queue: 1, when: 1}, checkIndexResult);
    self.dbQitems.createIndex({done: 1}, {sparse: true}, checkIndexResult);
    return done(null);

    function checkIndexResult(err, result) {
      if (err) return done(err);
      debug('created index', result);
    }
  });
};


DbWrkrMongoDB.prototype.disconnect = function disconnect(done) {
  if (!this.db) return done();

  this.db.close();

  this.dbSubscriptions = null;
  this.dbQitems = null;
  this.db = null;

  return done(null);
};



DbWrkrMongoDB.prototype.subscribe = function subscribe(eventName, queueName, done) {
  debug('subscribe ', {event: eventName, queue: queueName});
  var update = {
    $addToSet: {queues: queueName}
  };
  updateSubscription(this.dbSubscriptions, eventName, update, done);
};


DbWrkrMongoDB.prototype.unsubscribe = function unsubscribe(eventName, queueName, done) {
  debug('unsubscribe ', {event: eventName, queue: queueName});
  var update = {
    $pull: {queues: queueName}
  };
  updateSubscription(this.dbSubscriptions, eventName, update, done);
};


DbWrkrMongoDB.prototype.subscriptions = function subscriptions(eventName, done) {
  this.dbSubscriptions.findOne({eventName: eventName}, function (err, d) {
    if (err) return done(err);
    if (!d) return done(null, []);

    return done(null, d.queues);
  });
};



function updateSubscription(table, eventName, update, done) {
  var find = {eventName: eventName};
  var options = {
    upsert: true
  };
  table.findOneAndUpdate(find, update, options, function (err) {
    return done(err || null);
  });
}



DbWrkrMongoDB.prototype.publish = function publish(events, done) {
  var publishEvents = Array.isArray(events) ? events : [events];

  debug('storing ', publishEvents);
  this.dbQitems.insertMany(publishEvents, function (err, results) {
    if (err) return done(err);

    assert(results.result.n === publishEvents.length, 'all events stored');

    var createdIds = results.insertedIds.map( function (id) {
      return id+'';
    });
    return done(null, createdIds);
  });
};


DbWrkrMongoDB.prototype.fetchNext = function fetchNext(queue, done) {
  var query = {
    queue: queue,
    when: {$lte: new Date()}
  };
  var update = {
    $unset: {when: 1},
    $set: {done: new Date()}
  };
  var options = {
    sort: {'when': 1},
    returnOriginal: false
  };
  this.dbQitems.findOneAndUpdate(query, update, options, function (err, result) {
    if (err) return done(err);
    if (!result.value) return done(null, undefined);

    var nextItem = fieldMapper(result.value);
    debug('fetchNext', nextItem);
    return done(null, nextItem);
  });
};


DbWrkrMongoDB.prototype.find = function find(criteria, done) {
  if (criteria.id) {
    criteria._id = mongo.ObjectId(criteria.id);
    delete criteria.id;
  }

  debug('finding ', criteria);
  this.dbQitems.find(criteria).toArray(function (err, items) {
    if (err) return done(err);

    return done(null, items.map(fieldMapper));
  });
};


DbWrkrMongoDB.prototype.remove = function remove(criteria, done) {
  if (criteria.id) {
    criteria._id = mongo.ObjectId(criteria.id);
    delete criteria.id;
  }

  debug('removing', criteria);
  this.dbQitems.remove(criteria, function (err) {
    return done(err || null);
  });
};


function fieldMapper(qitem) {
  // TODO: More dynamic please
  return {
    id: qitem._id.toString(),
    name: qitem.name,
    queue: qitem.queue,
    tid: qitem.tid,
    created: qitem.created,
    when: qitem.when,
    done: qitem.done,
    retryCount: qitem.retryCount,
    parent: qitem.parent,
    payload: qitem.payload
  };
}


module.exports = DbWrkrMongoDB;
