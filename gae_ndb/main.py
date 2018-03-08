import webapp2

from google.appengine.ext import ndb


class Task(ndb.Model):
  description = ndb.TextProperty()


class ListEntitiesHandler(webapp2.RequestHandler):

  def get(self):
    """Writes the number of Greeting entities in response."""
    tasks = Task.query().fetch()
    for task in tasks:
      self.response.write(task)
      self.response.write('\n')


class PutEntityHandler(webapp2.RequestHandler):

  def post(self):
    """Write one Greeting entity to datastore."""
    names = self.request.get('names')
    if not names:
      self.response.write('name should not be empty!')
      self.response.set_status(400)
    else:
      names = names.split(',')
      for name in names:
        task = Task(description='From ndb')
        task.key = ndb.Key('Task', name)
        task.put()
        self.response.write('Successfully put data: %s\n' % task)


app = webapp2.WSGIApplication(
    [('/', ListEntitiesHandler),
     ('/list', ListEntitiesHandler),
     ('/put', PutEntityHandler)])
