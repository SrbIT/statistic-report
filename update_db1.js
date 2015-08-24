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

var updateRestaurants = function (paraCollection,
                                  paraTimeFormat,
                                  paraObject,
                                  paraSuff,
                                  paraTime,
                                  paraI,
                                  db, callback) {


    var vTimeFormatter, redisKey
    if (paraTime === 'HH') {
        vTimeFormatter = moment.utc().subtract(paraI, 'hours').format(paraTimeFormat)
        console.log(vTimeFormatter)

        redisKey = vTimeFormatter + paraObject
        console.log(redisKey)

    } else if (paraTime === 'dd') {
        vTimeFormatter = moment.utc().subtract(paraI, 'days').format(paraTimeFormat)
        console.log(vTimeFormatter)

        redisKey = vTimeFormatter + paraObject
        console.log(redisKey)
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


function UpdateData(paraCollection,
                    paraTimeFormat,
                    paraObject,
                    paraSuff,
                    paraTime,
                    paraI) {

    MongoClient.connect(url, function (err, db) {
        assert.equal(null, err);

        updateRestaurants(paraCollection,
            paraTimeFormat,
            paraObject,
            paraSuff,
            paraTime,
            paraI,
            db, function () {
                db.close();
            });
    });

}

var interValmm = setInterval(function () {

    for (var i = 35; i >= 5; i--) {
        UpdateData('tb_sessions_mm', "YYYYMMDDHHmm", ":session:", "_sessions_mm", "mm", i)

    }
    //UpdateData('tb_sessions_5m', "YYYYMMDDHH", ":session:", "_sessions_5m", "5m")
    for (var i = 23; i >= 0; i--) {
        UpdateData('tb_sessions_HH', "YYYYMMDDHH", ":session:", "_sessions_HH", "HH", i)
    }
    for (var i = 1; i >= 0; i--) {

        UpdateData('tb_sessions_dd', "YYYYMMDD", ":session:", "_sessions_dd", "dd", i)
    }

}, 20000)