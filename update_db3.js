/**
 * Created by soh-l on 10/08/2015.
 */

var MongoClient = require('mongodb').MongoClient;
var assert = require('assert')
var ObjectId = require('mongodb').ObjectID;
var url = 'mongodb://localhost:27017/db_la';
var _ = require('underscore');

var moment = require('moment');
var redis = require("redis");

var PORT = 6379,
//HOST = '127.0.0.1',
//    HOST = '10.0.0.24',
    HOST = 'r1',

    client_Redis = redis.createClient(PORT, HOST);

function ConvertDataZrevrange(reply) {

    var data = [];
    for (var i = 0; i < reply.length; i++) {
        var tmp = reply[i];
        var z = {};
        z.name = tmp[0];
        z.view = parseInt(tmp[1]);
        data.push(z);
    }
    return data

}

var updateMovieNameArray = function (paraCollection,
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
        redisKey = "stats:" + vTimeFormatter + paraObject
    }

    console.log(redisKey)
    client_Redis.zrevrange(redisKey, 0, 10, 'withscores', function (err, reply) {

        if (reply === null) {

            console.log("Error: " + redisKey + err)
            console.log("01")

        } else {

            var lists = _.groupBy(reply, function (a, b) {
                return Math.floor(b / 2);
            });
            var arr = _.toArray(lists);

            var data = ConvertDataZrevrange(arr)
            console.log(data);

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

function UpdateDataMovieNameArray(paraCollection,
                         paraTimeFormat,
                         paraObject,
                         paraTime,
                         paraI) {

    MongoClient.connect(url, function (err, db) {
        assert.equal(null, err);

        updateMovieNameArray(paraCollection,
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
    for (var i = 0; i >= 0; i--) {
        UpdateDataMovieNameArray('tb_movieName_dd', "YYYYMMDD", ":movieName:", "dd", i)
    }
}, 3000)