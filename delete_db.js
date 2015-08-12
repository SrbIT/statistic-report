/**
 * Created by soh-l on 11/08/2015.
 */
var MongoClient = require('mongodb').MongoClient;
var assert = require('assert')
var ObjectId = require('mongodb').ObjectID;
var url = 'mongodb://localhost:27017/db_la';

var removeRestaurants = function (db, callback) {
    db.collection('table_sessions').deleteMany(
        //{"date_min": "201508110521"},
        {"_id": "201508110521"},
        function (err, results) {
            console.log(results);
            callback();
        }
    );
};


MongoClient.connect(url, function (err, db) {
    assert.equal(null, err);

    removeRestaurants(db, function () {
        db.close();
    });
});