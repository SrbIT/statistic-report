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
        db.collection('tb_sessions_dd').aggregate(
            [
                //{$match: {"date_min": "201508"}},
                {$match: {"date_min": {$in: [/2015083/]}}},
                //{ $group: { "_id": "$date_min" , "count": { $sum: 1 } } }
                //{$group: {_id: "$_id", "total": {$sum: "$value"}}}
                {$group: {_id: "null", "total": {$sum: "$value"}}}
                //{
                //    $group: {"total": {"_id": "$_id", $sum: "$value"}}
                //}
            ]).
            toArray(function (err, result) {
                assert.equal(err, null);
                console.log(result);
                callback(result);
            }
        );
    }
    ;

MongoClient.connect(url, function (err, db) {
    assert.equal(null, err);
    aggregateRestaurants(db, function () {
        db.close();
    });
});
