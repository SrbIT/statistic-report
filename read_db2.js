/**
 * Created by soh-l on 10/08/2015.
 * Read data from mongodb to send client to show
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

app.get('/', function (req, res) {

    res.sendFile(path.join(__dirname + '/index2.html'));

});
app.get('/sta', function (req, res) {
    console.log("STA")

    res.sendFile(path.join(__dirname + '/index.html'));

});

var url = 'mongodb://localhost:27017/db_la';

io.on("connection", function (socket) {

    function getData(paraCollection,
                     paraChannel,
                     data) {
        var dataA = []
        var findData = function (db, callback) {
            var cursor = db.collection(paraCollection).find({}, {_id: 0}).sort({$natural: -1}).limit(30)
            cursor.each(function (err, doc) {
                assert.equal(err, null);
                if (doc != null) {
                    console.dir(doc);
                    dataA.push(doc);

                } else {
                    socket.emit(paraChannel, dataA)
                    console.log(dataA)
                    callback();
                }
            });

        };

        MongoClient.connect(url, function (err, db) {
            assert.equal(null, err);
            findData(db, function () {
                db.close();
            });
        });
    }

    function getDataArray(paraCollection,
                          paraChannel,
                          data) {
        var dataA = []
        var findDataArray = function (db, callback) {
            var cursor = db.collection(paraCollection).find({}, {_id: 0}).sort({$natural: -1}).limit(1)
            cursor.each(function (err, doc) {
                assert.equal(err, null);
                if (doc != null) {
                    console.dir(doc);
                    dataA.push(doc);

                } else {
                    socket.emit(paraChannel, dataA)
                    console.log(dataA)
                    callback();
                }
            });

        };

        MongoClient.connect(url, function (err, db) {
            assert.equal(null, err);
            findDataArray(db, function () {
                db.close();
            });
        });
    }

    function socketData(paraCollection,
                        paraChannelR,
                        paraChannelS) {

        socket.on(paraChannelR, function (data) {

            getData(paraCollection, paraChannelS, data)

        });

    }

    function socketDataArray(paraCollection,
                             paraChannelR,
                             paraChannelS) {

        socket.on(paraChannelR, function (data) {

            getDataArray(paraCollection, paraChannelS, data)

        });

    }

    socketData('tb_sessions_mm', "messagemm", "sessions_time")
    socketData('tb_sessions_5m', "message5m", "sessions_time")
    socketData('tb_sessions_HH', "messageHH", "sessions_time")
    socketData('tb_sessions_dd', "messagedd", "sessions_time")
    socketData('tb_sessions_MM', "messageMM", "sessions_time")

    socketDataArray('tb_product_mm', "productmm", "product_time")
    socketDataArray('tb_product_5m', "product5m", "product_time")
    socketDataArray('tb_product_HH', "productHH", "product_time")
    socketDataArray('tb_product_dd', "productdd", "product_time")
    socketDataArray('tb_product_MM', "productMM", "product_time")

    socketDataArray('tb_profile_mm', "profilemm", "profile_time")
    socketDataArray('tb_profile_5m', "profile5m", "profile_time")
    socketDataArray('tb_profile_HH', "profileHH", "profile_time")
    socketDataArray('tb_profile_dd', "profiledd", "profile_time")
    socketDataArray('tb_profile_MM', "profileMM", "profile_time")

    socketDataArray('tb_isp_mm', "ispmm", "isp_time")
    socketDataArray('tb_isp_5m', "isp5m", "isp_time")
    socketDataArray('tb_isp_HH', "ispHH", "isp_time")
    socketDataArray('tb_isp_dd', "ispdd", "isp_time")
    socketDataArray('tb_isp_MM', "ispMM", "isp_time")

    socketDataArray('tb_device_mm', "devicemm", "device_time")
    socketDataArray('tb_device_5m', "device5m", "device_time")
    socketDataArray('tb_device_HH', "deviceHH", "device_time")
    socketDataArray('tb_device_dd', "devicedd", "device_time")
    socketDataArray('tb_device_MM', "deviceMM", "device_time")

    socketDataArray('tb_info_mm', "infomm", "info_time")
    socketDataArray('tb_info_5m', "info5m", "info_time")
    socketDataArray('tb_info_HH', "infoHH", "info_time")
    socketDataArray('tb_info_dd', "infodd", "info_time")
    socketDataArray('tb_info_MM', "infoMM", "info_time")

});

server.listen(3000)
console.log("Server listening port: 3000")