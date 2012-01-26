var util = require('util');
var express = require('express'),
    namespace = require('express-namespace');
var uuid = require("node-uuid");

// command line arguments
var cli = require('commander');
cli
    .version('0.1')
    .option('-p, --port [port]', "port number to listen on (default: 8080)", parseInt)
    .option('--memcache_host')
    .parse(process.argv);

// setup memcache/hazelcast client
var memcache = require('memcache'),
    client = new memcache.Client();
client.connect();
function memcache_client_close() {
    client.close();
}

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

        // Put entry into memcache/Hazelcast:
        client.set(name + ":" + id, JSON.stringify(req.body));
        
        res.send(id);
    });
    
    app.get('/', function(req, res) {
        var name = req.params.name;
        var id = req.params.id;
        var data = client.get(name + ":" + id);
        if (data != null) {
            res.send(JSON.parse(client.get(id)));
        } else {
            res.send("Not found!", 404);
        }
    });
    
    app.del('/', function(req, res) {
        var name = req.params.name;
        var id = req.params.id;
        client.delete(name + ":" + id);
        res.send("Received DELETE for name:" + name + " and id:" + id);
    });
});

// make sure to close memcache client connection
process.on('SIGINT', function() {
    console.log("Shutting down.");
    memcache_client_close();
});

process.on('SIGTERM', function() {
    console.log("Shutting down.");
    memcache_client_close();
});


port = cli.port ? cli.port : 8080;
app.listen(port);
console.log("Server running on port %d.", port)