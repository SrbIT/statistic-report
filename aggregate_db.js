/**
 * Created by soh-l on 10/08/2015.
 */
var MongoClient = require('mongodb').MongoClient;
var assert = require('assert')
var ObjectId = require('mongodb').ObjectID;
var url = 'mongodb://localhost:27017/db_la';

var moment = require('moment');
//var redis = require("redis");
//
//var PORT = 6379,
//    HOST = '127.0.0.1',
//    client_Redis = redis.createClient(PORT, HOST);

//var vMinuteFormatter = moment.utc().subtract(1, 'minutes').format("YYYYMMDDHHmm")
//var vMinuteFormatter = moment.utc().subtract(3, 'minutes').format("YYYYMMDDHHmm")
//console.log(vMinuteFormatter)

var aggregateRestaurants = function (db, callback) {
    db.collection('table_sessions').aggregate(
        [
            //{$match: {"date_min": "201508100926"}},
            {$match: {"date_min": {$in: [/20150810092*/]}}},
            //{ $group: { "_id": "$date_min" , "count": { $sum: 1 } } }
            //{$group: {"_id": "$date_min", "total": {$sum: "$value"}}}
            {$group: {"_id": "20150810092", "total": {$sum: "$value"}}}
        ]).toArray(function (err, result) {
            assert.equal(err, null);
            console.log(result);
            callback(result);
        }
    );
};

MongoClient.connect(url, function (err, db) {
    assert.equal(null, err);
    aggregateRestaurants(db, function () {
        db.close();
    });
});

//var findAgg = function (db, callback) {
//    db.collection("table_sessions").aggregate([
//        {$match: {date_min: "201508100853"}},
//        {$group: {_id: "$date_min", total: {$sum: "$value"}}}
//    ], function (err, re) {
//        console.log(re)
//        db.close()
//    });

//console.log(cursor)
//var cursor =db.collection('table_sessions').find( );
//cursor.each(function (err, doc) {
//    assert.equal(err, null);
//    if (doc != null) {
//        console.dir(doc);
//    } else {
//        callback();
//    }
//});
//};

//MongoClient.connect(url, function (err, db) {
//    assert.equal(null, err);
//    findAgg(db, function () {
//        db.close();
//    });
//});