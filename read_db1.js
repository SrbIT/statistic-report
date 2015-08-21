/**
 * Created by soh-l on 10/08/2015.
 */

var MongoClient = require('mongodb').MongoClient;
var assert = require('assert')
var ObjectId = require('mongodb').ObjectID;

var express = require("express");
var http = require("http");
var path = require("path");

var socketio = require("socket.io");
var moment = require('moment');
var app = express();
var server = http.createServer(app);
var io = socketio.listen(server);

app.use(express.static("public"));
app.use(express.static(__dirname));

app.get('/sta', function (req, res) {
    console.log("STA")

    res.sendFile(path.join(__dirname + '/index9.html'));

});

var url = 'mongodb://localhost:27017/db_la';

io.on("connection", function (socket) {
    var interVal = setInterval(function () {
        var data = []
        var findRestaurants = function (db, callback) {
            var cursor = db.collection('table_sessions_mm').find({}, {_id: 0}).sort({$natural: -1}).limit(30)
            cursor.each(function (err, doc) {
                assert.equal(err, null);
                if (doc != null) {
                    console.dir(doc);
                    data.push(doc);

                } else {
                    socket.emit("echo1", data)
                    console.log(data)
                    callback();
                }
            });

        };

        MongoClient.connect(url, function (err, db) {
            assert.equal(null, err);
            findRestaurants(db, function () {
                db.close();
            });
        });

    }, 5000);

    socket.on("message1", function (data) {
        var data = []
        var findRestaurants = function (db, callback) {
            var cursor = db.collection('table_sessions_mm').find({}, {_id: 0}).sort({$natural: -1}).limit(30)
            cursor.each(function (err, doc) {
                assert.equal(err, null);
                if (doc != null) {
                    console.dir(doc);
                    data.push(doc);

                } else {
                    socket.emit("echo1", data)
                    console.log(data)
                    callback();
                }
            });

        };

        MongoClient.connect(url, function (err, db) {
            assert.equal(null, err);
            findRestaurants(db, function () {
                db.close();
            });
        });
        //socket.emit("echo1", data)

    });

    socket.on('disconnect', function () {
        clearInterval(interVal)
        console.log("Disconnect")
    });
});

server.listen(3000)
console.log("Server listening port: 3000")