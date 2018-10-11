#! /usr/bin/env python
#! -*- encoding: utf-8 -*-

from __future__ import print_function
from prometheus_client import start_http_server
from prometheus_client.core import CounterMetricFamily, REGISTRY
from elasticsearch import Elasticsearch
import time
import argparse

# addr = 'http://es01:9200/'
# index = 'nginx_*'

class Haproxy_Err_Status(object):

    def __init__(self, addr, index):
        self.addr = addr
        self.index = index
        self.es = Elasticsearch(self.addr)

    @property
    def searcher(self):
        _search = {
          "query": {
            "match": {
              "status": 500
            }
          },
          "aggs": {
            "count_path": {
              "terms": {
                "field": "path.keyword",
                "size": 100000
              }
            }
          }
        }
        return _search

    def collect(self):
        self.data = self.es.search(index=self.index, body=self.searcher)['aggregations']['count_path']['buckets']
        self.metrics = CounterMetricFamily('Haproxy_Error_500', 'The total number of haproxy error status 500, type: counter',
                               labels=['path'])
        for i in self.data:
            self.metrics.add_metric([i['key']], i['doc_count'])
        yield self.metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='connect es server and get some info from es')
    parser.add_argument('-a', '--addr', type=str, default='http://es01:9200/',
                        help='the es server with port, like: http://es:9200')

    parser.add_argument('-i', '--index', type=str, default="haproxy_access*",
                        help='the indica in es')

    parser.add_argument('-p', '--port', type=int, default=10000,
                        help='the listen port of this exporter.')

    args = vars(parser.parse_args())
    address = args['addr']
    index = args['index']
    port = args['port']

    REGISTRY.register(Haproxy_Err_Status(address, index))
    print('Start Haporxy Error Status exporter listen {0} on port {1}.......'.format(address, port))
    start_http_server(port)
    while True:
        time.sleep(5)

