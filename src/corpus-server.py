#!/usr/bin/python

import tornado.ioloop
import tornado.web

from config import RedisDB

class MainHandler(tornado.web.RequestHandler):
  def get(self):
    items = ["Item 1", "Item 2", "Item 3"]
    self.render("templates/corpus-index.html", title="KBA", items=items)

application = tornado.web.Application([
  (r"/", MainHandler),
])

if __name__ == "__main__":
  application.listen(8888)
  tornado.ioloop.IOLoop.instance().start()
