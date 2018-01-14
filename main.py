from __future__ import print_function
import uuid

import os
import pickle

import redis
import time
from multiprocessing import Process
from bluelens_spawning_pool import spawning_pool
from stylelens_product.products import Products
from stylelens_product.crawls import Crawls


from bluelens_log import Logging

INTERVAL_TIME = 60 * 10

# REDIS_PRODUCT_CLASSIFY_QUEUE = 'bl:product:classify:queue'
REDIS_PRODUCT_CLASSIFY_QUEUE = 'bl_product_classify_queue'
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

DB_OBJECT_FEATURE_HOST = os.environ['DB_OBJECT_FEATURE_HOST']
DB_OBJECT_FEATURE_PORT = os.environ['DB_OBJECT_FEATURE_PORT']
DB_OBJECT_FEATURE_NAME = os.environ['DB_OBJECT_FEATURE_NAME']
DB_OBJECT_FEATURE_USER = os.environ['DB_OBJECT_FEATURE_USER']
DB_OBJECT_FEATURE_PASSWORD = os.environ['DB_OBJECT_FEATURE_PASSWORD']

MAX_PROCESS_NUM = int(os.environ['MAX_PROCESS_NUM'])

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

rconn = redis.StrictRedis(REDIS_SERVER, decode_responses=True, port=6379, password=REDIS_PASSWORD)
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
  pool.addContainerEnv(container, 'MAX_PROCESS_NUM', str(MAX_PROCESS_NUM))
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
  pool.addContainerEnv(container, 'DB_OBJECT_FEATURE_HOST', DB_OBJECT_FEATURE_HOST)
  pool.addContainerEnv(container, 'DB_OBJECT_FEATURE_PORT', DB_OBJECT_FEATURE_PORT)
  pool.addContainerEnv(container, 'DB_OBJECT_FEATURE_USER', DB_OBJECT_FEATURE_USER)
  pool.addContainerEnv(container, 'DB_OBJECT_FEATURE_PASSWORD', DB_OBJECT_FEATURE_PASSWORD)
  pool.addContainerEnv(container, 'DB_OBJECT_FEATURE_NAME', DB_OBJECT_FEATURE_NAME)
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

def get_latest_crawl_version(rconn):
  value = rconn.hget(REDIS_CRAWL_VERSION, REDIS_CRAWL_VERSION_LATEST)
  if value is not None:
    version_id = value
    return version_id
  else:
    return None

def prepare_products_to_classfiy(rconn, version_id):
  global product_api
  offset = 0
  limit = 100

  clear_queue(rconn)
  remove_prev_pods()
  try:
    while True:
      res = product_api.get_products_by_version_id(version_id=version_id,
                                                   is_processed=True,
                                                   is_classified=False,
                                                   offset=offset,
                                                   limit=limit)
      log.debug("Got " + str(len(res)) + ' products')
      for product in res:
        rconn.lpush(REDIS_PRODUCT_CLASSIFY_QUEUE, pickle.dumps(product))

      if limit > len(res):
        break
      else:
        offset = offset + limit

  except Exception as e:
    log.error(str(e))

def remove_prev_pods():
  pool = spawning_pool.SpawningPool()
  pool.setServerUrl(REDIS_SERVER)
  pool.setServerPassword(REDIS_PASSWORD)
  pool.setMetadataNamespace(RELEASE_MODE)
  data = {}
  data['key'] = 'group'
  data['value'] = 'bl-object-classifier'
  pool.delete(data)
  data['value'] = 'bl-image-processor'
  pool.delete(data)
  time.sleep(60)

def clear_queue(rconn):
  rconn.delete(REDIS_PRODUCT_CLASSIFY_QUEUE)

def dispatch(rconn, version_id):
  log.info('dispatch')
  global product_api

  size = rconn.llen(REDIS_PRODUCT_CLASSIFY_QUEUE)

  if size < MAX_PROCESS_NUM:
    for i in range(10):
      spawn_classifier(str(uuid.uuid4()))
    # time.sleep(60*60*2)

  if size >= MAX_PROCESS_NUM and size < MAX_PROCESS_NUM*10:
    for i in range(200):
      spawn_classifier(str(uuid.uuid4()))
    # time.sleep(60*60*5)

  elif size >= MAX_PROCESS_NUM*100:
    for i in range(200):
      spawn_classifier(str(uuid.uuid4()))
    # time.sleep(60*60*10)

# def get_size_of_not_classified_products(product_api, version_id):
#   try:
#     size = product_api.get_size_not_classified(version_id)
#   except Exception as e:
#     log.error(str(e))
#   return size

def check_condition_to_start(version_id):
  global product_api

  product_api = Products()
  crawl_api = Crawls()

  try:
    # Check Crawling process is done
    total_crawl_size = crawl_api.get_size_crawls(version_id)
    crawled_size = crawl_api.get_size_crawls(version_id, status='done')
    if total_crawl_size != crawled_size:
      return False

    # Check Image processing process is done
    total_product_size = product_api.get_size_products(version_id)
    processed_size = product_api.get_size_products(version_id, is_processed=True)
    if total_product_size != processed_size:
      return False

    # Check Classifying processing process is done
    classified_size = product_api.get_size_products(version_id, is_classified=True)
    if total_product_size == classified_size:
      return False

    # Check Object classifying process is done
    queue_size = rconn.llen(REDIS_PRODUCT_CLASSIFY_QUEUE)
    if queue_size != 0:
      return False

  except Exception as e:
    log.error(str(e))

  return True

def start(rconn):
  while True:
    version_id = get_latest_crawl_version(rconn)
    if version_id is not None:
      log.info("check_condition_to_start")
      ok = check_condition_to_start(version_id)
      log.info("check_condition_to_start: " + str(ok))
      if ok is True:
        prepare_products_to_classfiy(rconn, version_id)
        dispatch(rconn, version_id)

    time.sleep(60*10)

if __name__ == '__main__':
  try:
    log.info("start bl-classify:2")
    Process(target=start, args=(rconn,)).start()
  except Exception as e:
    log.error(str(e))
