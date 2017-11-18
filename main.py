from __future__ import print_function
import uuid

import os
from multiprocessing import Process

import uuid
import logging
import redis
import json
import time
from bluelens_spawning_pool import spawning_pool
import stylelens_product
from stylelens_product.rest import ApiException

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

SPAWNING_CRITERIA = 10000

REDIS_HOST_CRAWL_QUEUE = 'bl:host:crawl:queue'
REDIS_PRODUCT_QUERY_QUEUE = 'bl:product:query:queue'
REDIS_PRODUCT_CLASSIFY_BUFFER = 'bl:product:classify:buffer'
REDIS_PRODUCT_CLASSIFY_QUEUE = 'bl:product:classify:queue'

REDIS_SERVER = os.environ['REDIS_SERVER']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']

AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

logging.basicConfig(filename='./log/main.log', level=logging.DEBUG)
rconn = redis.StrictRedis(REDIS_SERVER, port=6379, password=REDIS_PASSWORD)

def query(host_code):
  print('start query: ' + host_code)
  logging.debug(host_code)

  product_api = stylelens_product.ProductApi()

  try:
    res = product_api.get_products_by_hostcode(host_code)
    for p in res.data:
      push_product_to_queue(p)
  except ApiException as e:
    print(e)
    logging.error(e)

def push_product_to_queue(product):
  rconn.lpush(REDIS_PRODUCT_CLASSIFY_BUFFER, product.to_str())

def spawn_classifier(uuid):

  pool = spawning_pool.SpawningPool()

  project_name = 'bl-classifier-' + uuid
  print('spawn_classifier: ' + project_name)

  pool.setServerUrl(REDIS_SERVER)
  pool.setServerPassword(REDIS_PASSWORD)
  pool.setApiVersion('v1')
  pool.setKind('Pod')
  pool.setMetadataName(project_name)
  pool.setMetadataNamespace('index')
  pool.addMetadataLabel('name', project_name)
  pool.addMetadataLabel('SPAWN_ID', uuid)
  container = pool.createContainer()
  pool.setContainerName(container, project_name)
  pool.addContainerEnv(container, 'AWS_ACCESS_KEY', AWS_ACCESS_KEY)
  pool.addContainerEnv(container, 'AWS_SECRET_ACCESS_KEY', AWS_SECRET_ACCESS_KEY)
  pool.addContainerEnv(container, 'REDIS_SERVER', REDIS_SERVER)
  pool.addContainerEnv(container, 'REDIS_PASSWORD', REDIS_PASSWORD)
  pool.addContainerEnv(container, 'SPAWN_ID', uuid)
  pool.setContainerImage(container, 'bluelens/bl-classifier:latest')
  pool.addContainer(container)
  pool.setRestartPolicy('Never')
  # pool.spawn()

def dispatch_query_job(rconn):
  while True:
    key, value = rconn.blpop([REDIS_HOST_CRAWL_QUEUE])
    query(value.decode('utf-8'))

def dispatch_classifier(rconn):

  spawn_classifier(str(uuid.uuid4()))
  i = 1
  while True:
    key, value = rconn.blpop([REDIS_PRODUCT_CLASSIFY_BUFFER])
    rconn.lpush(REDIS_PRODUCT_CLASSIFY_QUEUE, value)
    if i % SPAWNING_CRITERIA == 0:
      spawn_classifier(str(uuid.uuid4()))
      time.sleep(60)
    i = i + 1

if __name__ == '__main__':
  Process(target=dispatch_query_job, args=(rconn,)).start()
  Process(target=dispatch_classifier, args=(rconn,)).start()
