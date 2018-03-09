#!/usr/bin/env python


"""Code snippests for r/w to Cloud Datastore Emulator using dummy credential."""


import argparse
import os

from google.auth import credentials
from google.cloud import datastore


DEFAULT_NAME = 'default_task'


# The kind for the new entity
ENTITY_KIND = 'Task'


def DoPut(datastore_client, name=None):
  """Put a list of entities with given key names.

  Args:
    datastore_client: An instance of datastore.Client.
    name: A string representing the name of the entity key.
  """
  if not name:
    print 'no entity name found, using the default name %s' % DEFAULT_NAME
    name = DEFAULT_NAME
  # The Cloud Datastore key for the new entity
  task_key = datastore_client.key(ENTITY_KIND, name)

  # Prepares the new entity
  task = datastore.Entity(key=task_key)
  task['description'] = 'From one platform'

  # Saves the entity
  datastore_client.put(task)
  print 'Successfully put entity of name:%r' % name


def DoList(datastore_client):
  """Returns a list of all entities of kind Task.

  Args:
    datastore_client: An instance of datastore.Client.
  """
  query = datastore_client.query(kind='Task')
  res = list(query.fetch())
  for e in res:
    print e


def build_datastore_client(use_real_credentials):
  if not 'DATASTORE_EMULATOR_HOST' in os.environ:
    raise RuntimeError(
        'Did not find DATASTORE_EMULATOR_HOST in environment variables. Cannot '
        'decide emulator host without it.')
  else:
    print 'Detected DATASTORE_EMULATOR_HOST=%s' % os.environ[
        'DATASTORE_EMULATOR_HOST']
  if use_real_credentials:
    # This requires running `gcloud auth application-default login` in advance.
    print 'WARNING: Watch out, you are using real credential! You may accidentally pollute production data!'
    return datastore.Client()
  else:
    if not 'DATASTORE_DATASET' in os.environ:
      raise RuntimeError(
          'Did not find DATASTORE_DATASET in environment variables. Cannot '
          'decide project id without it.')
    else:
      print 'Detected DATASTORE_DATASET=%s' % os.environ['DATASTORE_DATASET']
    # AnonymousCredentials do not provide any auth information. For more info:
    # http://google-auth.readthedocs.io/en/latest/reference/google.auth.credentials.html
    return datastore.Client(credentials=credentials.AnonymousCredentials())


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('method',
                      nargs=1,
                      help='The method to trigger. Either put or list.')
  parser.add_argument('--name',
                      nargs='?',
                      help='The name of new entity key.')
  parser.add_argument(
      '--real-credentials', dest='use_real_credentials', action='store_true')
  parser.set_defaults(use_real_credentials=False)

  options = parser.parse_args()
  method = options.method[0]

  client = build_datastore_client(options.use_real_credentials)
  if method == 'list':
    DoList(client)
  else:
    DoPut(client, options.name)
