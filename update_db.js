/**
 * Created by soh-l on 10/08/2015.
 */

var MongoClient = require('mongodb').MongoClient;
var assert = require('assert')
var ObjectId = require('mongodb').ObjectID;
var url = 'mongodb://localhost:27017/db_la';

var updateRestaurants = function(db, callback) {
    db.collection('table_sessions').updateOne(
        { "date_min" : "201508100926" },
        {
            $set: { "value": 503 }
        }, function(err, results) {
            console.log(results);
            callback();
        });
};

MongoClient.connect(url, function(err, db) {
    assert.equal(null, err);

    updateRestaurants(db, function() {
        db.close();
    });
});