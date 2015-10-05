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
//HOST = '127.0.0.1',
//HOST = '10.0.0.26',
    HOST = '23.99.96.65',

    client_Redis = redis.createClient(PORT, HOST);


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

var updateRestaurants = function (paraCollection,
                                  paraTimeFormat,
                                  paraObject,
                                  paraTime,
                                  paraI,
                                  db, callback) {


    var vTimeFormatter, redisKey
    if (paraTime === 'mm') {
        vTimeFormatter = moment.utc().subtract(paraI, 'minutes').format(paraTimeFormat)
        redisKey = vTimeFormatter + paraObject
    } else if (paraTime === 'HH') {
        vTimeFormatter = moment.utc().subtract(paraI, 'hours').format(paraTimeFormat)
        redisKey = vTimeFormatter + paraObject
    } else if (paraTime === 'dd') {
        vTimeFormatter = moment.utc().subtract(paraI, 'days').format(paraTimeFormat)
        redisKey = vTimeFormatter + paraObject
    }

    client_Redis.hlen(redisKey, function (err, reply) {

        if (reply === null) {

            console.log("Error: " + err)
            console.log(01)

        } else {
            console.log(parseInt(reply))

            db.collection(paraCollection).updateOne(
                {"date_min": vTimeFormatter},
                {
                    $set: {"value": parseInt(reply)}
                },
                {upsert: true}, function (err, results) {
                    //console.log(results);
                    callback();
                });

        }

    })

};

var updateRestaurantsArray = function (paraCollection,
                                       paraTimeFormat,
                                       paraObject,
                                       paraTime,
                                       paraI,
                                       db, callback) {

    var vTimeFormatter, redisKey

    if (paraTime === 'mm') {
        vTimeFormatter = moment.utc().subtract(paraI, 'minutes').format(paraTimeFormat)
        redisKey = vTimeFormatter + paraObject
    } else if (paraTime === 'HH') {
        vTimeFormatter = moment.utc().subtract(paraI, 'hours').format(paraTimeFormat)
        redisKey = vTimeFormatter + paraObject
    } else if (paraTime === 'dd') {
        vTimeFormatter = moment.utc().subtract(paraI, 'days').format(paraTimeFormat)
        redisKey = vTimeFormatter + paraObject
    }

    console.log(redisKey)
    client_Redis.hgetall(redisKey, function (err, reply) {

        if (reply === null) {

            console.log("Error: " + redisKey + err)
            console.log("01")

        } else {

            var data = ConvertData(reply)

            db.collection(paraCollection).updateOne(
                {"date_min": vTimeFormatter},
                {
                    $set: {"value": data}
                },
                {upsert: true}, function (err, results) {
                    //console.log(results);
                    callback();
                });

        }

    })

};

function UpdateData(paraCollection,
                    paraTimeFormat,
                    paraObject,
                    paraTime,
                    paraI) {

    MongoClient.connect(url, function (err, db) {
        assert.equal(null, err);

        updateRestaurants(paraCollection,
            paraTimeFormat,
            paraObject,
            paraTime,
            paraI,
            db, function () {
                db.close();
            });
    });

}

function UpdateDataArray(paraCollection,
                         paraTimeFormat,
                         paraObject,
                         paraTime,
                         paraI) {

    MongoClient.connect(url, function (err, db) {
        assert.equal(null, err);

        updateRestaurantsArray(paraCollection,
            paraTimeFormat,
            paraObject,
            paraTime,
            paraI,
            db, function () {
                db.close();
            });
    });

}

var interValmm = setInterval(function () {

    for (var i = 25; i >= 5; i--) {

        UpdateData('tb_sessions_mm', "YYYYMMDDHHmm", ":session:", "mm", i)
        UpdateData('tb_played_mm', "YYYYMMDDHHmm", ":played:", "mm", i)

        UpdateDataArray('tb_product_mm', "YYYYMMDDHHmm", ":product:", "mm", i)
        UpdateDataArray('tb_profile_mm', "YYYYMMDDHHmm", ":profile:", "mm", i)
        UpdateDataArray('tb_isp_mm', "YYYYMMDDHHmm", ":isp:", "mm", i)
        UpdateDataArray('tb_device_mm', "YYYYMMDDHHmm", ":device:", "mm", i)
        UpdateDataArray('tb_info_mm', "YYYYMMDDHHmm", ":info:", "mm", i)

    }
    for (var i = 1; i >= 0; i--) {

        UpdateData('tb_sessions_HH', "YYYYMMDDHH", ":session:", "HH", i)
        UpdateData('tb_played_HH', "YYYYMMDDHH", ":played:", "HH", i)

        UpdateDataArray('tb_product_HH', "YYYYMMDDHH", ":product:", "HH", i)
        UpdateDataArray('tb_profile_HH', "YYYYMMDDHH", ":profile:", "HH", i)
        UpdateDataArray('tb_isp_HH', "YYYYMMDDHH", ":isp:", "HH", i)
        UpdateDataArray('tb_device_HH', "YYYYMMDDHH", ":device:", "HH", i)
        UpdateDataArray('tb_info_HH', "YYYYMMDDHH", ":info:", "HH", i)

    }
    for (var i = 1; i >= 0; i--) {

        UpdateData('tb_sessions_dd', "YYYYMMDD", ":session:", "dd", i)
        UpdateData('tb_played_dd', "YYYYMMDD", ":played:", "dd", i)

        UpdateDataArray('tb_product_dd', "YYYYMMDD", ":product:", "dd", i)
        UpdateDataArray('tb_profile_dd', "YYYYMMDD", ":profile:", "dd", i)
        UpdateDataArray('tb_isp_dd', "YYYYMMDD", ":isp:", "dd", i)
        UpdateDataArray('tb_device_dd', "YYYYMMDD", ":device:", "dd", i)
        UpdateDataArray('tb_info_dd', "YYYYMMDD", ":info:", "dd", i)

    }

}, 60000)