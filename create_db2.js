/**
 * Created by soh-l on 11/08/2015.
 * Read data from redis to drop mogodb
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

var insertDocumentSession = function (paraCollection,
                                      paraTimeFormat,
                                      paraObject,
                                      paraSuff,
                                      paraTime,
                                      db, callback) {

    var vTimeFormatter, redisKey

    if (paraTime === 'mm') {
        vTimeFormatter = moment.utc().subtract(5, 'minutes').format(paraTimeFormat)
        //var vMinuteFormatter = "201508110506"
        redisKey = vTimeFormatter + paraObject
    } else if (paraTime === '5m') {
        vTimeFormatter = moment.utc().format(paraTimeFormat).toString() + (Math.floor(moment().utc().minutes() / 5) + 1).toString();
        console.log(vTimeFormatter)
        redisKey = vTimeFormatter + paraObject
    }
    else if (paraTime === 'HH') {
        vTimeFormatter = moment.utc().subtract(1, 'hours').format(paraTimeFormat)
        redisKey = vTimeFormatter + paraObject
    } else if (paraTime === 'dd') {
        vTimeFormatter = moment.utc().format(paraTimeFormat)
        redisKey = vTimeFormatter + paraObject
    } else if (paraTime === 'MM') {
        vTimeFormatter = moment.utc().format(paraTimeFormat)
        redisKey = vTimeFormatter + paraObject
    } else {
        vTimeFormatter = moment.utc().format(paraTimeFormat)
        redisKey = vTimeFormatter + paraObject
    }


    console.log(redisKey)

    client_Redis.hlen(redisKey, function (err, reply) {

        if (reply === null) {

            console.log("Error: " + err)
            console.log(0)

            db.collection(paraCollection).insertOne({
                "_id": vTimeFormatter + paraSuff,
                "date_min": vTimeFormatter,
                "value": 0
            }, function (err, result) {
                assert.equal(err, null);
                console.log("Inserted a document null(0).");
                callback(result);
            });

        } else {

            db.collection(paraCollection).insertOne({

                "_id": vTimeFormatter + paraSuff,
                "date_min": vTimeFormatter,
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

var insertDocumentArray = function (paraCollection,
                                    paraTimeFormat,
                                    paraObject,
                                    paraSuff,
                                    paraTime,
                                    db, callback) {


    var vTimeFormatter, redisKey

    if (paraTime === 'mm') {
        vTimeFormatter = moment.utc().subtract(5, 'minutes').format(paraTimeFormat)
        //var vMinuteFormatter = "201508110506"
        redisKey = vTimeFormatter + paraObject
    } else if (paraTime === '5m') {
        vTimeFormatter = moment.utc().format(paraTimeFormat).toString() + (Math.floor(moment().utc().minutes() / 5) + 1).toString();
        console.log(vTimeFormatter)
        redisKey = vTimeFormatter + paraObject
    } else if (paraTime === 'HH') {
        vTimeFormatter = moment.utc().subtract(1, 'hours').format(paraTimeFormat)
        redisKey = vTimeFormatter + paraObject
    } else if (paraTime === 'dd') {
        vTimeFormatter = moment.utc().format(paraTimeFormat)
        redisKey = vTimeFormatter + paraObject
    } else if (paraTime === 'MM') {
        vTimeFormatter = moment.utc().format(paraTimeFormat)
        redisKey = vTimeFormatter + paraObject
    } else {
        vTimeFormatter = moment.utc().format(paraTimeFormat)
        redisKey = vTimeFormatter + paraObject
    }

    console.log(redisKey)

    client_Redis.hgetall(redisKey, function (err, reply) {

        if (reply === null) {

            var tmp = [{name: "soh1", y: 0},
                {name: "soh2", y: 0},
                {name: "soh3", y: 0}]
            console.log("E" + err)

            db.collection(paraCollection).insertOne({
                "_id": vTimeFormatter + paraSuff,
                "date_min": vTimeFormatter,
                "value": tmp
            }, function (err, result) {
                assert.equal(err, null);
                console.log("Inserted a document default product.");
                callback(result);
            });


        } else {

            var data = ConvertData(reply)

            db.collection(paraCollection).insertOne({
                "_id": vTimeFormatter + paraSuff,
                "date_min": vTimeFormatter,
                "value": data
            }, function (err, result) {
                assert.equal(err, null);
                console.log("Inserted a document get redis.");
                callback(result);
            });

        }

    })

};

function ConvertData(reply) {

    var keys = Object.keys(reply);
    var data = [];
    for (var i = 0; i < keys.length; i++) {
        var z = {};
        z.name = keys[i];
        z.y = parseInt(reply[keys[i]]);
        data.push(z);
    }
    return data

}

var interValmm = setInterval(function () {

    // Session
    MongoClient.connect(url, function (err, db) {
        assert.equal(null, err);

        var paraTimeFormat = "YYYYMMDDHHmm",
            paraTime = 'mm'

        insertDocumentSession('tb_sessions_mm',
            paraTimeFormat,
            ":session:",
            "_sessions_mm",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentSession('tb_session_hdo_mm',
            paraTimeFormat,
            ":session:hdo:",
            "_session_hdo_mm",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentSession('tb_session_hdviet_mm',
            paraTimeFormat,
            ":session:hdviet:",
            "_session_hdviet_mm",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentSession('tb_session_vip_hdviet_mm',
            paraTimeFormat,
            ":session:vip_hdviet:",
            "_session_vip_hdviet_mm",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentArray('tb_product_mm',
            paraTimeFormat,
            ":product:",
            "_product_mm",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentArray('tb_profile_mm',
            paraTimeFormat,
            ":profile:",
            "_profile_mm",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentArray('tb_isp_mm',
            paraTimeFormat,
            ":isp:",
            "_isp_mm",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentArray('tb_device_mm',
            paraTimeFormat,
            ":device:",
            "_device_mm",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentArray('tb_info_mm',
            paraTimeFormat,
            ":info:",
            "_info_mm",
            paraTime,
            db, function () {
                //db.close();
            });

    })


}, 60000)

var interVal5m = setInterval(function () {

    // Session
    MongoClient.connect(url, function (err, db) {
        assert.equal(null, err);

        var paraTimeFormat = "YYYYMMDDHH",
            paraTime = '5m'

        insertDocumentSession('tb_sessions_5m',
            paraTimeFormat,
            ":session:",
            "_sessions_5m",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentSession('tb_session_hdo_5m',
            paraTimeFormat,
            ":session:hdo:",
            "_session_hdo_5m",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentSession('tb_session_hdviet_5m',
            paraTimeFormat,
            ":session:hdviet:",
            "_session_hdviet_5m",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentSession('tb_session_vip_hdviet_5m',
            paraTimeFormat,
            ":session:vip_hdviet:",
            "_session_vip_hdviet_5m",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentArray('tb_product_5m',
            paraTimeFormat,
            ":product:",
            "_product_5m",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentArray('tb_profile_5m',
            paraTimeFormat,
            ":profile:",
            "_profile_5m",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentArray('tb_isp_5m',
            paraTimeFormat,
            ":isp:",
            "_isp_5m",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentArray('tb_device_5m',
            paraTimeFormat,
            ":device:",
            "_device_5m",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentArray('tb_info_5m',
            paraTimeFormat,
            ":info:",
            "_info_5m",
            paraTime,
            db, function () {
                //db.close();
            });

    })


}, 300000)

var interValHH = setInterval(function () {

    // Session
    MongoClient.connect(url, function (err, db) {
        assert.equal(null, err);

        var paraTimeFormat = "YYYYMMDDHH",
            paraTime = 'HH'

        insertDocumentSession('tb_sessions_HH',
            paraTimeFormat,
            ":session:",
            "_sessions_HH",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentSession('tb_session_hdo_HH',
            paraTimeFormat,
            ":session:hdo:",
            "_session_hdo_HH",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentSession('tb_session_hdviet_HH',
            paraTimeFormat,
            ":session:hdviet:",
            "_session_hdviet_HH",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentSession('tb_session_vip_hdviet_HH',
            paraTimeFormat,
            ":session:vip_hdviet:",
            "_session_vip_hdviet_HH",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentArray('tb_product_HH',
            paraTimeFormat,
            ":product:",
            "_product_HH",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentArray('tb_profile_HH',
            paraTimeFormat,
            ":profile:",
            "_profile_HH",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentArray('tb_isp_HH',
            paraTimeFormat,
            ":isp:",
            "_isp_HH",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentArray('tb_device_HH',
            paraTimeFormat,
            ":device:",
            "_device_HH",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentArray('tb_info_HH',
            paraTimeFormat,
            ":info:",
            "_info_HH",
            paraTime,
            db, function () {
                //db.close();
            });

    })


}, 3600000)

var interValdd = setInterval(function () {

    // Session
    MongoClient.connect(url, function (err, db) {
        assert.equal(null, err);

        var paraTimeFormat = "YYYYMMDD",
            paraTime = 'dd'

        insertDocumentSession('tb_sessions_dd',
            paraTimeFormat,
            ":session:",
            "_sessions_dd",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentSession('tb_session_hdo_dd',
            paraTimeFormat,
            ":session:hdo:",
            "_session_hdo_dd",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentSession('tb_session_hdviet_dd',
            paraTimeFormat,
            ":session:hdviet:",
            "_session_hdviet_dd",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentSession('tb_session_vip_hdviet_dd',
            paraTimeFormat,
            ":session:vip_hdviet:",
            "_session_vip_hdviet_dd",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentArray('tb_product_dd',
            paraTimeFormat,
            ":product:",
            "_product_dd",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentArray('tb_profile_dd',
            paraTimeFormat,
            ":profile:",
            "_profile_dd",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentArray('tb_isp_dd',
            paraTimeFormat,
            ":isp:",
            "_isp_dd",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentArray('tb_device_dd',
            paraTimeFormat,
            ":device:",
            "_device_dd",
            paraTime,
            db, function () {
                //db.close();
            });

        insertDocumentArray('tb_info_dd',
            paraTimeFormat,
            ":info:",
            "_info_dd",
            paraTime,
            db, function () {
                //db.close();
            });

    })

}, 86400000)

