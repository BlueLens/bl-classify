from __future__ import print_function
import uuid

import os
import pickle

import redis
import time
from bluelens_spawning_pool import spawning_pool
from stylelens_product.products import Products

from bluelens_log import Logging


REDIS_PRODUCT_QUERY_QUEUE = 'bl:product:query:queue'
REDIS_PRODUCT_CLASSIFY_QUEUE = 'bl:product:classify:queue'
REDIS_CRAWL_VERSION = 'bl:crawl:version'
REDIS_CRAWL_VERSION_LATEST = 'latest'
REDIS_PRODUCT_IMAGE_PROCESS_QUEUE = 'bl:product:image:process:queue'

REDIS_SERVER = os.environ['REDIS_SERVER']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
RELEASE_MODE = os.environ['RELEASE_MODE']
OD_HOST = os.environ['OD_HOST']
OD_PORT = os.environ['OD_PORT']
DB_OBJECT_HOST = os.environ['DB_OBJECT_HOST']
DB_OBJECT_PORT = os.environ['DB_OBJECT_PORT']
DB_OBJECT_NAME = os.environ['DB_OBJECT_NAME']
DB_OBJECT_USER = os.environ['DB_OBJECT_USER']
DB_OBJECT_PASSWORD = os.environ['DB_OBJECT_PASSWORD']

DB_PRODUCT_HOST = os.environ['DB_PRODUCT_HOST']
DB_PRODUCT_PORT = os.environ['DB_PRODUCT_PORT']
DB_PRODUCT_USER = os.environ['DB_PRODUCT_USER']
DB_PRODUCT_PASSWORD = os.environ['DB_PRODUCT_PASSWORD']
DB_PRODUCT_NAME = os.environ['DB_PRODUCT_NAME']

DB_IMAGE_HOST = os.environ['DB_IMAGE_HOST']
DB_IMAGE_PORT = os.environ['DB_IMAGE_PORT']
DB_IMAGE_NAME = os.environ['DB_IMAGE_NAME']
DB_IMAGE_USER = os.environ['DB_IMAGE_USER']
DB_IMAGE_PASSWORD = os.environ['DB_IMAGE_PASSWORD']

AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

rconn = redis.StrictRedis(REDIS_SERVER, port=6379, password=REDIS_PASSWORD)
options = {
  'REDIS_SERVER': REDIS_SERVER,
  'REDIS_PASSWORD': REDIS_PASSWORD
}
log = Logging(options, tag='bl-classify')

product_api = None


def spawn_classifier(uuid):

  pool = spawning_pool.SpawningPool()

  project_name = 'bl-object-classifier-' + uuid
  log.debug('spawn_classifier: ' + project_name)

  pool.setServerUrl(REDIS_SERVER)
  pool.setServerPassword(REDIS_PASSWORD)
  pool.setApiVersion('v1')
  pool.setKind('Pod')
  pool.setMetadataName(project_name)
  pool.setMetadataNamespace(RELEASE_MODE)
  pool.addMetadataLabel('name', project_name)
  pool.addMetadataLabel('group', 'bl-object-classifier')
  pool.addMetadataLabel('SPAWN_ID', uuid)
  container = pool.createContainer()
  pool.setContainerName(container, project_name)
  pool.addContainerEnv(container, 'AWS_ACCESS_KEY', AWS_ACCESS_KEY)
  pool.addContainerEnv(container, 'AWS_SECRET_ACCESS_KEY', AWS_SECRET_ACCESS_KEY)
  pool.addContainerEnv(container, 'REDIS_SERVER', REDIS_SERVER)
  pool.addContainerEnv(container, 'REDIS_PASSWORD', REDIS_PASSWORD)
  pool.addContainerEnv(container, 'SPAWN_ID', uuid)
  pool.addContainerEnv(container, 'RELEASE_MODE', RELEASE_MODE)
  pool.addContainerEnv(container, 'OD_HOST', OD_HOST)
  pool.addContainerEnv(container, 'OD_PORT', OD_PORT)
  pool.addContainerEnv(container, 'DB_PRODUCT_HOST', DB_PRODUCT_HOST)
  pool.addContainerEnv(container, 'DB_PRODUCT_PORT', DB_PRODUCT_PORT)
  pool.addContainerEnv(container, 'DB_PRODUCT_USER', DB_PRODUCT_USER)
  pool.addContainerEnv(container, 'DB_PRODUCT_PASSWORD', DB_PRODUCT_PASSWORD)
  pool.addContainerEnv(container, 'DB_PRODUCT_NAME', DB_PRODUCT_NAME)
  pool.addContainerEnv(container, 'DB_OBJECT_HOST', DB_OBJECT_HOST)
  pool.addContainerEnv(container, 'DB_OBJECT_PORT', DB_OBJECT_PORT)
  pool.addContainerEnv(container, 'DB_OBJECT_USER', DB_OBJECT_USER)
  pool.addContainerEnv(container, 'DB_OBJECT_PASSWORD', DB_OBJECT_PASSWORD)
  pool.addContainerEnv(container, 'DB_OBJECT_NAME', DB_OBJECT_NAME)
  pool.addContainerEnv(container, 'DB_IMAGE_HOST', DB_IMAGE_HOST)
  pool.addContainerEnv(container, 'DB_IMAGE_PORT', DB_IMAGE_PORT)
  pool.addContainerEnv(container, 'DB_IMAGE_USER', DB_IMAGE_USER)
  pool.addContainerEnv(container, 'DB_IMAGE_PASSWORD', DB_IMAGE_PASSWORD)
  pool.addContainerEnv(container, 'DB_IMAGE_NAME', DB_IMAGE_NAME)
  pool.setContainerImage(container, 'bluelens/bl-object-classifier:' + RELEASE_MODE)
  pool.setContainerImagePullPolicy(container, 'Always')
  pool.addContainer(container)
  pool.setRestartPolicy('Never')
  pool.spawn()

def get_latest_crawl_version():
  value = rconn.hget(REDIS_CRAWL_VERSION, REDIS_CRAWL_VERSION_LATEST)
  if value is not None:
    version_id = value.decode("utf-8")
    return version_id
  else:
    return None

def dispatch_classifier():
  product_api = Products()
  offset = 0
  limit = 500

  try:
    while True:
      res = product_api.get_products_by_version_id(version_id=version_id,
                                                   is_processed=True,
                                                   is_classified=False,
                                                   offset=offset,
                                                   limit=limit)

      hash = {}
      for product in res:
        hash[str(product['_id'])] = product

      rconn.hmset(REDIS_PRODUCT_CLASSIFY_QUEUE, pickle.dumps(hash))

      log.debug("Got " + str(len(res)) + 'products')

      if len(res) == 0:
        break
      else:
        offset = offset + limit
        spawn_classifier(str(uuid.uuid4()))
        time.sleep(60)

  except Exception as e:
    log.error(str(e))

if __name__ == '__main__':
  try:
    while True:
      if rconn.llen(REDIS_PRODUCT_IMAGE_PROCESS_QUEUE) > 0:
        version_id = get_latest_crawl_version()
        dispatch_classifier()
  except Exception as e:
    log.error(str(e))
