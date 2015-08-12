/**
 * Created by soh-l on 10/08/2015.
 */

var MongoClient = require('mongodb').MongoClient;
var assert = require('assert')
var ObjectId = require('mongodb').ObjectID;
var url = 'mongodb://localhost:27017/db_la';

var moment = require('moment');
var redis = require("redis");

var PORT = 6379,
    HOST = '127.0.0.1',
    client_Redis = redis.createClient(PORT, HOST);

//var vMinuteFormatter = moment.utc().subtract(1, 'minutes').format("YYYYMMDDHHmm")
//var vMinuteFormatterUTC = moment.utc().subtract(1, 'minutes').format()
var vMinuteFormatter = "201508110521"
console.log(vMinuteFormatter)
var redisSessionKey = vMinuteFormatter + ":session:"
console.log(redisSessionKey)

var insertDocument = function (db, callback) {

    db.collection('table_sessions').insertOne({
        "_id": vMinuteFormatter,
        "date_min": vMinuteFormatter,
        "value": 56
    }, function (err, result) {
        assert.equal(err, null);
        console.log("Inserted a document into the restaurants collection.");
        client_Redis.quit();
        callback(result);
    });
};

MongoClient.connect(url, function (err, db) {
    assert.equal(null, err);
    insertDocument(db, function () {
        db.close();
    });
})