from __future__ import print_function
import uuid

import os
from multiprocessing import Process

import redis
import time
import pickle
from bluelens_spawning_pool import spawning_pool
import stylelens_product
from stylelens_product.rest import ApiException

from bluelens_log import Logging


HOST_URL = 'host_url'
TAG = 'tag'
SUB_CATEGORY = 'sub_category'
PRODUCT_NAME = 'product_name'
IMAGE_URL = 'image'
PRODUCT_PRICE = 'product_price'
CURRENCY_UNIT = 'currency_unit'
PRODUCT_URL = 'product_url'
PRODUCT_NO = 'product_no'
MAIN = 'main'
NATION = 'nation'

REDIS_HOST_CLASSIFY_QUEUE = 'bl:host:classify:queue'
REDIS_PRODUCT_QUERY_QUEUE = 'bl:product:query:queue'
REDIS_PRODUCT_CLASSIFY_BUFFER = 'bl:product:classify:buffer'
REDIS_PRODUCT_CLASSIFY_QUEUE = 'bl:product:classify:queue'
REDIS_PRODUCT_IMAGE_PROCESS_QUEUE = 'bl:product:image:process:queue'
# REDIS_PRODUCT_HASH = 'bl:product:hash'
REDIS_CRAWL_VERSION = 'bl:crawl:version'
REDIS_CRAWL_VERSION_LATEST = 'latest'

REDIS_SERVER = os.environ['REDIS_SERVER']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
RELEASE_MODE = os.environ['RELEASE_MODE']
OD_HOST = os.environ['OD_HOST']
OD_PORT = os.environ['OD_PORT']

AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

rconn = redis.StrictRedis(REDIS_SERVER, port=6379, password=REDIS_PASSWORD)
options = {
  'REDIS_SERVER': REDIS_SERVER,
  'REDIS_PASSWORD': REDIS_PASSWORD
}
log = Logging(options, tag='bl-classify')

def get_latest_crawl_version():
  value = rconn.hget(REDIS_CRAWL_VERSION, REDIS_CRAWL_VERSION_LATEST)
  version_id = value.decode("utf-8")
  return version_id

def query(host_code):
  log.info('start query: ' + host_code)

  version_id = get_latest_crawl_version()
  product_api = stylelens_product.ProductApi()

  q_offset = 0
  q_limit = 100

  try:
    while True:
      res = product_api.get_products_by_hostcode_and_version_id(host_code, version_id,
                                                                is_indexed=False,
                                                                offset=q_offset, limit=q_limit)
      for p in res.data:
        push_product_to_queue(p)
      if q_limit > len(res.data):
        break
      else:
        q_offset = q_offset + q_limit
  except ApiException as e:
    log.error(str(e) + ':' + host_code)

def cleanup_products(host_code, version_id):
  product_api = stylelens_product.ProductApi()
  try:
    res = product_api.delete_products_by_hostcode_and_version_id(host_code, version_id,
                                                              except_version=True)
    log.debug(res)
  except ApiException as e:
    log.error(e)

def push_product_to_queue(product):
  rconn.lpush(REDIS_PRODUCT_IMAGE_PROCESS_QUEUE, pickle.dumps(product.to_dict()))

def spawn_classifier(uuid):

  pool = spawning_pool.SpawningPool()

  project_name = 'bl-classifier-' + uuid
  log.debug('spawn_classifier: ' + project_name)

  pool.setServerUrl(REDIS_SERVER)
  pool.setServerPassword(REDIS_PASSWORD)
  pool.setApiVersion('v1')
  pool.setKind('Pod')
  pool.setMetadataName(project_name)
  pool.setMetadataNamespace(RELEASE_MODE)
  pool.addMetadataLabel('name', project_name)
  pool.addMetadataLabel('group', 'bl-classifier')
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
  pool.setContainerImage(container, 'bluelens/bl-classifier:' + RELEASE_MODE)
  pool.addContainer(container)
  pool.setRestartPolicy('Never')
  pool.spawn()

def dispatch_query_job(rconn):
  while True:
    key, value = rconn.blpop([REDIS_HOST_CLASSIFY_QUEUE])
    query(value.decode('utf-8'))

def dispatch_classifier(rconn):

  while True:
    len = rconn.llen(REDIS_PRODUCT_CLASSIFY_QUEUE)
    if len > 0:
      spawn_classifier(str(uuid.uuid4()))
    time.sleep(60)

if __name__ == '__main__':
  # dispatch_query_job(rconn)
  Process(target=dispatch_query_job, args=(rconn,)).start()
  Process(target=dispatch_classifier, args=(rconn,)).start()

  # query('HC0006')