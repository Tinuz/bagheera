var express = require('express'),
    namespace = require('express-namespace');
var uuid = require("node-uuid");
var redis = require("redis"),
    client = redis.createClient();
        
var app = express.createServer();

app.configure(function() {
    app.use(express.bodyParser());
});

app.namespace('/submit/:name/:id', function() {
    app.post('/', function(req, res) {
        req.accepts('application/json');
        var name = req.params.name;
        var id = req.params.id;
        if (id == null) {
            id = uuid.v4();
        }
        client.select(name);
        client.set(id, JSON.stringify(req.body));
        
        res.send(id);
    });
    
    app.get('/', function(req, res) {
        var name = req.params.name;
        var id = req.params.id;
        client.select(name);
        var data = client.get(id);
        if (data != null) {
            res.send(JSON.parse(client.get(id)));
        } else {
            res.send("Not found!", 404);
        }
    });
    
    app.del('/', function(req, res) {
        var name = req.params.name;
        var id = req.params.id;
        //client.select(name);
        res.send("Received DELETE for name:" + name + " and id:" + id);
    });
});

app.listen(8080);