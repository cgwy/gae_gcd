#!/usr/bin/env python


"""Code snippests for r/w to Cloud Datastore Emulator using dummy credential."""


import argparse
import os
import time

import google.auth.credentials
from google.cloud import datastore


# Inspired by: https://github.com/GoogleCloudPlatform/google-cloud-python/ \
# blob/11e310ab9f63f06252ff2b9ada201fd8c11ce06c/test_utils/test_utils/system.py
class EmulatorCreds(google.auth.credentials.Credentials):
  """A mock credential object.

  Used to avoid unnecessary token refreshing or reliance on the network
  while an emulator is running.
  """

  def __init__(self):  # pylint: disable=super-init-not-called
    self.token = b'seekrit'
    self.expiry = None

  @property
  def valid(self):
    return True

  def refresh(self, unused_request):  # pylint: disable=unused-argument
    """Off-limits implementation for abstract method."""
    raise RuntimeError('Should never be refreshed.')


DEFAULT_NAME = 'default_task'


# DATASTORE_EMULATOR_HOST points library to emulator host
# DATASTORE_DATASET decides the project to write to. It overrides gcloud config
# and gcloud auth application-default login.
RELAVANT_ENV_VARS = ['DATASTORE_EMULATOR_HOST', 'DATASTORE_DATASET']


for env_var in RELAVANT_ENV_VARS:
  print '%s=%s' % (env_var, os.environ[env_var])


# The kind for the new entity
ENTITY_KIND = 'Task'


def DoPut(datastore_client, names=()):
  """Put a list of entities with given key names.

  Args:
    datastore_client: An instance of datastore.Client.
    names: A list of strings representing the name of the entity key.
  """
  if not names:
    print 'no entity names found, using the default name %s' % DEFAULT_NAME
    names = [DEFAULT_NAME]
  for name in names:
    # The Cloud Datastore key for the new entity
    task_key = datastore_client.key(ENTITY_KIND, name)

    # Prepares the new entity
    task = datastore.Entity(key=task_key)
    task['description'] = 'From one platform'

    # Saves the entity
    datastore_client.put(task)
  print 'Successfully put entities of names:%r' % names


def DoList(datastore_client):
  """Returns a list of all entities of kind Task.

  Args:
    datastore_client: An instance of datastore.Client.
  """
  query = datastore_client.query(kind='Task')
  res = list(query.fetch())
  for e in res:
    print e


def build_datastore_client(dummy_credentials):
  # From https://github.com/GoogleCloudPlatform/google-cloud-python/blob/ \
  # e272dd0fb417cc3730238b9af4a759c2a1a3e96f/datastore/tests/unit/test_client.py
  # Instantiates a client
  if dummy_credentials:
    # This will determine project id from env var DATASTORE_DATASET
    # Alternatively, you can overwrite project id with parameter:
    # project=<string project id>,
    return datastore.Client(credentials=EmulatorCreds())
  else:
    return datastore.Client()


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('method',
                      nargs=1,
                      help='The method to trigger. Either put or list.')
  parser.add_argument('--names',
                      nargs='*',
                      help='The name of new entity keys.')
  parser.add_argument('--use_dummy_credentials',
                      nargs='?',
                      help='If True, use dummy credential.')
  parser.add_argument(
      '--no-dummy-credentials', dest='dummy_credentials', action='store_false')
  parser.set_defaults(dummy_credentials=True)

  options = parser.parse_args()
  method = options.method[0]

  client = build_datastore_client(options.dummy_credentials)
  if method == 'list':
    DoList(client)
  else:
    DoPut(client, options.names or [])
