/**
 * Created by soh-l on 10/08/2015.
 */

var MongoClient = require('mongodb').MongoClient;
var assert = require('assert')
var ObjectId = require('mongodb').ObjectID;

var url = 'mongodb://localhost:27017/db_la';

var findRestaurants = function (db, callback) {
    //var cursor = db.collection('table_sessions').find({}, {_id: 0}).limit( 5 )
    //var cursor = db.collection('table_sessions').find({}, {_id: 0}).skip( 5 )
    //var cursor = db.collection('table_sessions').find({}, {_id: 0}).sort( { date_min: 1 } )
    //var cursor = db.collection('table_sessions').find({}, {_id: 0}).limit( 5 ).sort( { date_min: 1 } )
    //var cursor = db.collection('table_sessions').find().sort( { date_min: 1 }).limit( 5 )
    var cursor = db.collection('table_sessions').find({}, {_id: 0}).sort({$natural: -1}).limit(5)
    //var cursor = db.collection('table_sessions').find({$query:{},$orderby : {$natural : -1}}).limit(5).sort({$natural: -1})
    //var cursor = db.collection('table_sessions').find({}, { date_min: { $slice: 7 } } )
    var data = []
    //console.log(cursor)
    console.log("_______________")
    cursor.each(function (err, doc) {
        assert.equal(err, null);
        if (doc != null) {
            console.dir(doc);
            data.push(doc);

        } else {
            console.log(data)
            callback();
        }
        //
    });

};

MongoClient.connect(url, function (err, db) {
    assert.equal(null, err);
    findRestaurants(db, function () {
        db.close();
    });
});