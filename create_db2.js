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

function InsertData(paraCollection,
                    paraTimeFormat,
                    paraObject,
                    paraSuff,
                    paraTime) {

    MongoClient.connect(url, function (err, db) {
        assert.equal(null, err);
        insertDocumentSession(paraCollection,
            paraTimeFormat,
            paraObject,
            paraSuff,
            paraTime,
            db, function () {
                db.close();
            });
    })
}


function InsertDataArray(paraCollection,
                         paraTimeFormat,
                         paraObject,
                         paraSuff,
                         paraTime) {

    MongoClient.connect(url, function (err, db) {
        assert.equal(null, err);
        insertDocumentArray(paraCollection,
            paraTimeFormat,
            paraObject,
            paraSuff,
            paraTime,
            db, function () {
                db.close();
            });
    })
}

var interValmm = setInterval(function () {

    InsertData('tb_sessions_mm', "YYYYMMDDHHmm", ":session:", "_sessions_mm", "mm")
    InsertData('tb_session_hdo_mm', "YYYYMMDDHHmm", ":session:hdo:", "_session_hdo_mm", "mm")
    InsertData('tb_session_hdviet_mm', "YYYYMMDDHHmm", ":session:hdviet:", "_session_hdviet_mm", "mm")
    InsertData('tb_session_vip_hdviet_mm', "YYYYMMDDHHmm", ":session:vip_hdviet:", "_session_vip_hdviet_mm", "mm")

    InsertDataArray('tb_product_mm', "YYYYMMDDHHmm", ":product:", "_product_mm", "mm")
    InsertDataArray('tb_profile_mm', "YYYYMMDDHHmm", ":profile:", "_profile_mm", "mm")
    InsertDataArray('tb_isp_mm', "YYYYMMDDHHmm", ":isp:", "_isp_mm", "mm")
    InsertDataArray('tb_device_mm', "YYYYMMDDHHmm", ":device:", "_device_mm", "mm")
    InsertDataArray('tb_info_mm', "YYYYMMDDHHmm", ":info:", "_info_mm", "mm")

}, 60000)

var interVal5m = setInterval(function () {

    InsertData('tb_sessions_5m', "YYYYMMDDHH", ":session:", "_sessions_5m", "5m")
    InsertData('tb_session_hdo_5m', "YYYYMMDDHH", ":session:hdo:", "_session_hdo_5m", "5m")
    InsertData('tb_session_hdviet_5m', "YYYYMMDDHH", ":session:hdviet:", "_session_hdviet_5m", "5m")
    InsertData('tb_session_vip_hdviet_5m', "YYYYMMDDHH", ":session:vip_hdviet:", "_session_vip_hdviet_5m", "5m")

    InsertDataArray('tb_product_5m', "YYYYMMDDHH", ":product:", "_product_5m", "5m")
    InsertDataArray('tb_profile_5m', "YYYYMMDDHH", ":profile:", "_profile_5m", "5m")
    InsertDataArray('tb_isp_5m', "YYYYMMDDHH", ":isp:", "_isp_5m", "5m")
    InsertDataArray('tb_device_5m', "YYYYMMDDHH", ":device:", "_device_5m", "5m")
    InsertDataArray('tb_info_5m', "YYYYMMDDHH", ":info:", "_info_5m", "5m");

}, 300000)

var interValHH = setInterval(function () {

    InsertData('tb_sessions_HH', "YYYYMMDDHH", ":session:", "_sessions_HH", "HH")
    InsertData('tb_session_hdo_HH', "YYYYMMDDHH", ":session:hdo:", "_session_hdo_HH", "HH")
    InsertData('tb_session_hdviet_HH', "YYYYMMDDHH", ":session:hdviet:", "_session_hdviet_HH", "HH")
    InsertData('tb_session_vip_hdviet_HH', "YYYYMMDDHH", ":session:vip_hdviet:", "_session_vip_hdviet_HH", "HH")

    InsertDataArray('tb_product_HH', "YYYYMMDDHH", ":product:", "_product_HH", "HH")
    InsertDataArray('tb_profile_HH', "YYYYMMDDHH", ":profile:", "_profile_HH", "HH")
    InsertDataArray('tb_isp_HH', "YYYYMMDDHH", ":isp:", "_isp_HH", "HH")
    InsertDataArray('tb_device_HH', "YYYYMMDDHH", ":device:", "_device_HH", "HH")
    InsertDataArray('tb_info_HH', "YYYYMMDDHH", ":info:", "_info_HH", "HH");

}, 3600000)

var interValdd = setInterval(function () {


    InsertData('tb_sessions_dd', "YYYYMMDD", ":session:", "_sessions_dd", "dd")
    InsertData('tb_session_hdo_dd', "YYYYMMDD", ":session:hdo:", "_session_hdo_dd", "dd")
    InsertData('tb_session_hdviet_dd', "YYYYMMDD", ":session:hdviet:", "_session_hdviet_dd", "dd")
    InsertData('tb_session_vip_hdviet_dd', "YYYYMMDD", ":session:vip_hdviet:", "_session_vip_hdviet_dd", "dd")

    InsertDataArray('tb_product_dd', "YYYYMMDD", ":product:", "_product_dd", "dd")
    InsertDataArray('tb_profile_dd', "YYYYMMDD", ":profile:", "_profile_dd", "dd")
    InsertDataArray('tb_isp_dd', "YYYYMMDD", ":isp:", "_isp_dd", "dd")
    InsertDataArray('tb_device_dd', "YYYYMMDD", ":device:", "_device_dd", "dd")
    InsertDataArray('tb_info_dd', "YYYYMMDD", ":info:", "_info_dd", "dd");

}, 86400000)

