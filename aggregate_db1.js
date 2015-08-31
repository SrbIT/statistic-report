/**
 * Created by soh-l on 10/08/2015.
 */
var MongoClient = require('mongodb').MongoClient;
var assert = require('assert')
var ObjectId = require('mongodb').ObjectID;
var url = 'mongodb://localhost:27017/db_la';

var moment = require('moment');

function getArrayTime() {
    var arr = []
    for (var i = 0; i < 7; i++) {
        var vMinuteFormatter = moment.utc().subtract(i, 'days').format("YYYYMMDD")
        arr.push(new RegExp(vMinuteFormatter))
    }
    return arr;
}
console.log(getArrayTime())

var aggregateRestaurants = function (db, callback) {
    db.collection('tb_isp_mm').aggregate(
        [
            {$unwind: "$value"},
            {$match: {"date_min": {$in: getArrayTime()}}},
            {
                $group: {
                    _id: "$value.name",
                    "y": {$sum: "$value.y"}
                }
            },
            {
                $project: {
                    'name': "$_id",
                    'y': "$y",
                    _id: 0
                }
            }

        ]).
        toArray(function (err, result) {
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
