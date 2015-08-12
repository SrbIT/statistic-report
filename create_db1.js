/**
 * Created by soh-l on 11/08/2015.
 */

var MongoClient = require('mongodb').MongoClient;
var assert = require('assert')
var ObjectId = require('mongodb').ObjectID;
var url = 'mongodb://localhost:27017/db_la';

var moment = require('moment');
var redis = require("redis");

var PORT = 6379,
    //HOST = '127.0.0.1',
    HOST = '127.0.0.1',
    client_Redis = redis.createClient(PORT, HOST);


var insertDocument = function (db, callback) {

    //var vMinuteFormatter = moment.utc().subtract(5, 'minutes').format("YYYYMMDDHHmm")
    var vMinuteFormatter = "201508110506"
    console.log(vMinuteFormatter)
    var redisSessionKey = vMinuteFormatter + ":session:"
    console.log(redisSessionKey)

    client_Redis.hlen(redisSessionKey, function (err, reply) {
        console.log("Error: " + err)

        if (reply === null) {

            console.log(0)

            db.collection('table_sessions_mm').insertOne({
                "_id": vMinuteFormatter,
                "date_min": vMinuteFormatter,
                "value": 0
            }, function (err, result) {
                assert.equal(err, null);
                console.log("Inserted a document null(0).");
                callback(result);
            });

        } else {

            console.log(reply)

            db.collection('table_sessions_mm').insertOne({
                "_id": vMinuteFormatter,
                "date_min": vMinuteFormatter,
                "value": parseInt(reply)
            }, function (err, result) {
                assert.equal(err, null);
                console.log("Inserted a document get redis.");
                callback(result);
            });

        }

        //client_Redis.quit();

    })

};
var interVal = setInterval(function () {
    MongoClient.connect(url, function (err, db) {
        assert.equal(null, err);
        insertDocument(db, function () {
            db.close();
        });
    })
},60000)