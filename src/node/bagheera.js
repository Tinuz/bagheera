var express = require('express'),
    namespace = require('express-namespace');
var redis = require("redis"),
    client = redis.createClient();
        
var app = express.createServer();

app.configure(function() {
    app.use(express.bodyParser());
});

app.namespace('/:name/:id', function() {
    app.post('/', function(req, res) {
        var name = req.params.name;
        var id = req.params.id;
        client.lpush(id, req.body);
    });
    
    app.get('/', function(req, res) {
        var name = req.params.name;
        var id = req.params.id;
        res.send("Received GET for name:" + name + " and id:" + id);
    });
    
    app.del('/', function(req, res) {
        var name = req.params.name;
        var id = req.params.id;
        res.send("Received DELETE for name:" + name + " and id:" + id);
    });
});

app.listen(8080);