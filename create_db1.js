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
//HOST = '10.0.0.26',
    HOST = '23.99.96.65',

    client_Redis = redis.createClient(PORT, HOST);

var insertDocumentSession = function (db, callback) {

    var vMinuteFormatter = moment.utc().subtract(5, 'minutes').format("YYYYMMDDHHmm")
    //var vMinuteFormatter = "201508110506"
    var redisSessionKey = vMinuteFormatter + ":session:"

    console.log(redisSessionKey)

    client_Redis.hlen(redisSessionKey, function (err, reply) {

        if (reply === null) {

            console.log("Error: " + err)
            console.log(0)

            db.collection('table_sessions_mm').insertOne({
                "_id": vMinuteFormatter + "sessions_mm",
                "date_min": vMinuteFormatter,
                "value": 0
            }, function (err, result) {
                assert.equal(err, null);
                console.log("Inserted a document null(0).");
                callback(result);
            });

        } else {

            db.collection('table_sessions_mm').insertOne({
                "_id": vMinuteFormatter + "_sessions_mm",
                "date_min": vMinuteFormatter,
                "value": parseInt(reply)
            }, function (err, result) {
                assert.equal(err, null);
                console.log("Inserted a document sessions get redis.");
                callback(result);
            });

        }

        //client_Redis.quit();

    })

};

var insertDocumentProduct = function (db, callback) {

    var vMinuteFormatter = moment.utc().subtract(5, 'minutes').format("YYYYMMDDHHmm")
    //var vMinuteFormatter = "201508110506"
    var redisProductKey = vMinuteFormatter + ":product:"

    console.log(redisProductKey)

    client_Redis.hgetall(redisProductKey, function (err, reply) {

        if (reply === null) {

            var tmp = [{name: "hdo", y: 0},
                {name: "hdviet", y: 0},
                {name: "vip_hdviet", y: 0}]
            console.log("E" + err)

            db.collection('table_product_mm').insertOne({
                "_id": vMinuteFormatter + "_product_mm",
                "date_min": vMinuteFormatter,
                "value": tmp
            }, function (err, result) {
                assert.equal(err, null);
                console.log("Inserted a document default product.");
                callback(result);
            });


        } else {

            var keys = Object.keys(reply);
            var data = [];
            for (var i = 0; i < keys.length; i++) {
                var z = {};
                z.name = keys[i];
                z.y = parseInt(reply[keys[i]]);
                data.push(z);
            }

            db.collection('table_product_mm').insertOne({
                "_id": vMinuteFormatter + "_product_mm",
                "date_min": vMinuteFormatter,
                "value": data
            }, function (err, result) {
                assert.equal(err, null);
                console.log("Inserted a document product get redis.");
                callback(result);
            });

        }

    })

};

var insertDocumentISP = function (db, callback) {

    var vMinuteFormatter = moment.utc().subtract(5, 'minutes').format("YYYYMMDDHHmm")
    //var vMinuteFormatter = "201508110506"
    var redisISPKey = vMinuteFormatter + ":isp"

    console.log(redisISPKey)

    client_Redis.hgetall(redisISPKey, function (err, reply) {

        if (reply === null) {

            var tmp = [{name: "vnpt", y: 0},
                {name: "fpt", y: 0},
                {name: "other", y: 0}]
            console.log("E" + err)

            db.collection('table_isp_mm').insertOne({
                "_id": vMinuteFormatter + "_isp_mm",
                "date_min": vMinuteFormatter,
                "value": tmp

            }, function (err, result) {
                assert.equal(err, null);
                console.log("Inserted a document default isp.");
                callback(result);
            });

        } else {

            var keys = Object.keys(reply);
            var data = [];
            for (var i = 0; i < keys.length; i++) {
                var z = {};
                z.name = keys[i];
                z.y = parseInt(reply[keys[i]]);
                data.push(z);
            }

            db.collection('table_isp_mm').insertOne({
                "_id": vMinuteFormatter + "_isp_mm",
                "date_min": vMinuteFormatter,
                "value": data
            }, function (err, result) {
                assert.equal(err, null);
                console.log("Inserted a document isp get redis.");
                callback(result);
            });

        }

    })

};

var insertDocumentInfo = function (db, callback) {

    var vMinuteFormatter = moment.utc().subtract(5, 'minutes').format("YYYYMMDDHHmm")
    //var vMinuteFormatter = "201508110506"
    var redisInfoKey = vMinuteFormatter + ":info"

    console.log(redisInfoKey)

    client_Redis.hgetall(redisInfoKey, function (err, reply) {

        if (reply === null) {

            var tmp = [{name: "hot", y: 0},
                {name: "warm", y: 0},
                {name: "cool", y: 0}]
            console.log("E" + err)

            db.collection('table_info_mm').insertOne({
                "_id": vMinuteFormatter + "_info_mm",
                "date_min": vMinuteFormatter,
                "value": tmp
            }, function (err, result) {
                assert.equal(err, null);
                console.log("Inserted a document default info.");
                callback(result);
            });


        } else {

            var keys = Object.keys(reply);
            var data = [];
            for (var i = 0; i < keys.length; i++) {
                var z = {};
                z.name = keys[i];
                z.y = parseInt(reply[keys[i]]);
                data.push(z);
            }

            db.collection('table_info_mm').insertOne({
                "_id": vMinuteFormatter + "_info_mm",
                "date_min": vMinuteFormatter,
                "value": data
            }, function (err, result) {
                assert.equal(err, null);
                console.log("Inserted a document info get redis.");
                callback(result);
            });

        }

    })

};

var insertDocumentDevice = function (db, callback) {

    var vMinuteFormatter = moment.utc().subtract(5, 'minutes').format("YYYYMMDDHHmm")
    //var vMinuteFormatter = "201508110506"
    var redisDeviceKey = vMinuteFormatter + ":device"

    console.log(redisDeviceKey)

    client_Redis.hgetall(redisDeviceKey, function (err, reply) {

        if (reply === null) {

            var tmp = [{name: "pc", y: 1},
                {name: "iphone", y: 1},
                {name: "ipad", y: 1}]
            console.log("E" + err)

            db.collection('table_device_mm').insertOne({
                "_id": vMinuteFormatter + "_device_mm",
                "date_min": vMinuteFormatter,
                "value": tmp
            }, function (err, result) {
                assert.equal(err, null);
                console.log("Inserted a document default devices.");
                callback(result);
            });

        } else {

            var keys = Object.keys(reply);
            var data = [];
            for (var i = 0; i < keys.length; i++) {
                var z = {};
                z.name = keys[i];
                z.y = parseInt(reply[keys[i]]);
                data.push(z);
            }

            db.collection('table_device_mm').insertOne({
                "_id": vMinuteFormatter + "_device_mm",
                "date_min": vMinuteFormatter,
                "value": data
            }, function (err, result) {
                assert.equal(err, null);
                console.log("Inserted a document device get redis.");
                callback(result);
            });

        }

    })

};

var insertDocumentProfile = function (db, callback) {

    var vMinuteFormatter = moment.utc().subtract(5, 'minutes').format("YYYYMMDDHHmm")
    //var vMinuteFormatter = "201508110506"
    var redisProfileKey = vMinuteFormatter + ":profile"

    console.log(redisProfileKey)

    client_Redis.hgetall(redisProfileKey, function (err, reply) {

        if (reply === null) {

            var tmp = [{name: "720p", y: 0},
                {name: "480p", y: 0},
                {name: "360p", y: 0}]
            console.log("E" + err)

            db.collection('table_profile_mm').insertOne({
                "_id": vMinuteFormatter + "_profile_mm",
                "date_min": vMinuteFormatter,
                "value": tmp
            }, function (err, result) {
                assert.equal(err, null);
                console.log("Inserted a document default profile.");
                callback(result);
            });

        } else {

            var keys = Object.keys(reply);
            var data = [];
            for (var i = 0; i < keys.length; i++) {
                var z = {};
                z.name = keys[i];
                z.y = parseInt(reply[keys[i]]);
                data.push(z);
            }

            db.collection('table_profile_mm').insertOne({
                "_id": vMinuteFormatter + "_profile_mm",
                "date_min": vMinuteFormatter,
                "value": data
            }, function (err, result) {
                assert.equal(err, null);
                console.log("Inserted a document profile get redis.");
                callback(result);
            });

        }

    })

};

var interVal = setInterval(function () {

    // Session
    MongoClient.connect(url, function (err, db) {
        assert.equal(null, err);
        insertDocumentSession(db, function () {
            db.close();
        });
    })

    // Product
    MongoClient.connect(url, function (err, db) {
        assert.equal(null, err);
        insertDocumentProduct(db, function () {
            db.close();
        });
    })

    // ISP
    MongoClient.connect(url, function (err, db) {
        assert.equal(null, err);
        insertDocumentISP(db, function () {
            db.close();
        });
    })

    // Info
    MongoClient.connect(url, function (err, db) {
        assert.equal(null, err);
        insertDocumentInfo(db, function () {
            db.close();
        });
    })

    // Device
    MongoClient.connect(url, function (err, db) {
        assert.equal(null, err);
        insertDocumentDevice(db, function () {
            db.close();
        });
    })

    // Profile
    MongoClient.connect(url, function (err, db) {
        assert.equal(null, err);
        insertDocumentProfile(db, function () {
            db.close();
        });
    })

}, 60000)