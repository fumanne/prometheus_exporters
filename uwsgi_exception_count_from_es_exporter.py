#! /usr/local/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function
from prometheus_client import start_http_server
from prometheus_client.core import CounterMetricFamily, REGISTRY
from elasticsearch import Elasticsearch
import time
import argparse

class UWSGI_Exception(object):
    def __init__(self, addr, index):
        self.addr = addr
        self.index = index
        self.es = Elasticsearch(self.addr)

    @property
    def _search(self):
        _search = {
            "query": {},
                "aggs": {
                    "count_path": {
                        "terms": {
                            "field": "path.keyword",
                            "size": 100000
                        },
                        "aggs": {
                            "count_message": {
                                "terms": {
                                    "field": "message.keyword"
                                }
                            }
                        }
                    }
                }
        }
        return _search

    def collect(self):

        full_data = self.es.search(index=self.index, body=self._search)
        if 'aggregations' in full_data:
            self.data = full_data['aggregations']['count_path']['buckets']
            self.metrics = CounterMetricFamily('UWSGI_Exception_counts', 'the total number of Exceptions',
                                       labels=['path', 'exception'])

            for item in self.data:
                for info in item['count_message']['buckets']:
                    self.metrics.add_metric([item['key'], info['key']], info['doc_count'])

            yield self.metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='connect es server and get some info from es')
    parser.add_argument('-a', '--addr', type=str, default='http://es01:9200/',
                        help='the es server with port, like: http://es01:9200')

    parser.add_argument('-i', '--index', type=str, default="uwsgi_penguin_prod-*",
                        help='the indica in es')

    parser.add_argument('-p', '--port', type=int, default=10002,
                        help='the listen port of this exporter.')

    args = vars(parser.parse_args())
    address = args['addr']
    index = args['index']
    port = args['port']

    REGISTRY.register(UWSGI_Exception(address, index))
    print('Start UWSGI_Exception Count exporter listen {0} on port {1}.......'.format(address, port))
    start_http_server(port)
    while True:
        time.sleep(5)
