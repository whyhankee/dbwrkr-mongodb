'use strict';
const util = require('util');

const assert = require('assert');
const debug = require('debug')('dbwrkr:mongodb');
const mongo = require('mongodb');

const mongoClient = mongo.MongoClient;


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
  const self = this;

  const cnStr = util.format('mongodb://%s:%s/%s',
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

    self.dbQitems.createIndex({name: 1, tid: 1}, checkIndexResult);
    self.dbQitems.createIndex({queue: 1, created: 1}, {
      partialFilterExpression: {
        when: {$exists: 1}
      }
    }, checkIndexResult);

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
  const update = {
    $addToSet: {queues: queueName}
  };
  updateSubscription(this.dbSubscriptions, eventName, update, done);
};


DbWrkrMongoDB.prototype.unsubscribe = function unsubscribe(eventName, queueName, done) {
  debug('unsubscribe ', {event: eventName, queue: queueName});
  const update = {
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
  const find = {eventName: eventName};
  const options = {
    upsert: true
  };
  table.findOneAndUpdate(find, update, options, function (err) {
    return done(err || null);
  });
}



DbWrkrMongoDB.prototype.publish = function publish(events, done) {
  const publishEvents = Array.isArray(events) ? events : [events];

  debug('storing ', publishEvents);
  this.dbQitems.insertMany(publishEvents, function (err, results) {
    if (err) return done(err);

    assert(results.result.n === publishEvents.length, 'all events stored');

    const createdIds = results.insertedIds.map(function (id) {
      return id+'';
    });
    return done(null, createdIds);
  });
};


DbWrkrMongoDB.prototype.fetchNext = function fetchNext(queue, done) {
  const query = {
    queue: queue,
    when: {$lte: new Date()}
  };
  const update = {
    $unset: {when: 1},
    $set: {done: new Date()}
  };
  const options = {
    sort: {'created': 1},
    returnOriginal: false
  };
  this.dbQitems.findOneAndUpdate(query, update, options, function (err, result) {
    if (err) return done(err);
    if (!result.value) return done(null, undefined);

    const nextItem = fieldMapper(result.value);
    debug('fetchNext', nextItem);
    return done(null, nextItem);
  });
};


// Note: only findByID should find processed qitem
DbWrkrMongoDB.prototype.find = function find(criteria, done) {
  if (criteria.id) {
    let findIds = Array.isArray(criteria.id) ? criteria.id : [criteria.id];
    findIds = findIds.map(function (id) {
      return mongo.ObjectId(id);
    });
    criteria._id = {$in: findIds} ;
    delete criteria.id;
  }

  if (!criteria._id) {
    criteria.when = {$gte: new Date(0, 0, 0)};
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
