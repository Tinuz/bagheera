import tornado.ioloop
import tornado.web
import redis
import leveldb

class RedisHandler(tornado.web.RequestHandler):
    
    SUPPORTED_METHODS = ("GET", "POST", "DELETE")
    
    def initialize(self, db_dict):
        self.db_dict = db_dict
        
    def get(self, namespace, id):
        self.write("Called get with namespace=%s and id=%s\n" % (namespace, id))
        item = self.redis_client.lpop(namespace)
        if item != None:
            self.write(item)
        else:
            self.write("No value found!")
            
    def post(self, namespace, id):
        self.write("Called post with namespace=%s, id=%s and body:\n %s"% (namespace, id, self.request.body))
        if namespace in self.db_dict:
            db = self.db_dict[namespace]
        else:
            db = redis.Redis(host='localhost', port=6379, db=len(self.db_dict))
            self.db_dict[namespace] = db
            
        self.db.rpush(id, self.request.body)

class LevelDBHandler(tornado.web.RequestHandler):

    SUPPORTED_METHODS = ("GET", "POST", "DELETE")
    
    def initialize(self, db_dict):
        self.db_dict = db_dict

    def get(self, namespace, id):
        self.write("Called get with namespace=%s and id=%s\n" % (namespace, id))
        if namespace in self.db_dict:
            db = self.db_dict[namespace]
            item = db.Get(id)
            if item != None:
                self.write(item)
                
    def post(self, namespace, id):
        self.write("Called post with namespace=%s, id=%s and body:\n %s"% (namespace, id, self.request.body))
        if namespace in self.db_dict:
            db = self.db_dict[namespace]
        else:
            db = leveldb.LevelDB("./" + namespace)
            self.db_dict[namespace] = db
        
        db.Put(id, self.request.body)
        
redis_db_dict = {}
level_db_dict = {}
application = tornado.web.Application([ 
    (r"/redis/([^/]+)/*([^/]*)", RedisHandler, dict(db_dict=redis_db_dict)),
    (r"/leveldb/([^/]+)/*([^/]*)", LevelDBHandler, dict(db_dict=level_db_dict))
])

if __name__ == "__main__":
    application.listen(8080)
    try:
        tornado.ioloop.IOLoop.instance().start()
    finally:
        for ns,db in redis_db_dict.iteritems():
            print "Closing db for %s" % (ns)
            db.close()