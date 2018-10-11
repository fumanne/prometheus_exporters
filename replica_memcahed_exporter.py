#! /usr/local/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function
import memcache
from prometheus_client import start_http_server
from prometheus_client.core import CounterMetricFamily, REGISTRY, GaugeMetricFamily
import time
import argparse

#addr = ['mc010.pg:11211', 'mc011.pg:11211']

class RepMemcached_Stats(object):
    def __init__(self, addr):
        if not isinstance(addr, list):
            raise ValueError("{0} type must be list".format(addr))

        self.m = memcache.Client(addr, debug=1)

    def collect(self):
        self.get_hits = CounterMetricFamily('repmemcached_get_hits', 'the information of get_hits',
                                            labels=['addr'])
        self.get_misses = CounterMetricFamily('repmemcached_get_misses', 'the information of get_misses',
                                            labels=['addr'])

        self.up_time = CounterMetricFamily('repmemcached_uptime', 'the timestamps of repmemcached running',
                                           labels=['addr'])

        self.curr_connections = GaugeMetricFamily('repmemcached_curr_connections', 'the current connections',
                                                  labels=['addr'])

        self.total_connections = CounterMetricFamily('repmemcached_total_connections', 'Numer of successful connect attempts to this server since it has been started',
                                                     labels=['addr'])

        self.cmd_get = CounterMetricFamily('repmemcached_cmd_get', 'the total number of get command',
                                           labels=['addr'])

        self.cmd_set = CounterMetricFamily('repmemcached_cmd_set', 'the total number of set command',
                                           labels=['addr'])

        self.evictions = GaugeMetricFamily('repmemcached_evictions', 'the value of evictions',
                                           labels=['addr'])

        self.repcached_qi_free = GaugeMetricFamily('repmemcached_repcached_qi_free', 'the current queue free',
                                                   labels=['addr'])

        self.memory_used = GaugeMetricFamily('repmemcached_memory_used', 'the memory used by total_malloced unit: byte',
                                             labels=['addr'])

        self.memory_max = GaugeMetricFamily('repmemcached_memory_max', 'the max value of memory settings, by running with -m parameter unit:byte',
                                            labels=['addr'])

        self.mem_up = GaugeMetricFamily('repmemcached_up', 'if the repmemcached is alive',
                                        labels=['addr'])


        for raw_address, info in dict(self.m.get_stats()).items():
            address = raw_address.split()[0]
            get_hits_value = info['get_hits']
            get_misses_value = info['get_misses']
            curr_connections_value = info['curr_connections']
            total_connections_value = info['total_connections']
            repcached_qi_free_value = info['repcached_qi_free']
            max_memory_value = info['limit_maxbytes']
            uptime_value = info['uptime']
            cmd_set_value = info['cmd_set']
            cmd_get_value = info['cmd_get']
            evictions_value = info['evictions']

            self.get_hits.add_metric([address], int(get_hits_value))
            self.get_misses.add_metric([address], int(get_misses_value))
            self.up_time.add_metric([address], int(uptime_value))
            self.curr_connections.add_metric([address], int(curr_connections_value))
            self.total_connections.add_metric([address], int(total_connections_value))
            self.repcached_qi_free.add_metric([address], int(repcached_qi_free_value))
            self.memory_max.add_metric([address], int(max_memory_value))
            self.cmd_get.add_metric([address], int(cmd_get_value))
            self.cmd_set.add_metric([address], int(cmd_set_value))
            self.evictions.add_metric([address], int(evictions_value))

        for host in self.m.servers:
            h, p = host.address
            address = ":".join([h, str(p)])
            self.mem_up.add_metric([address], host.connect())

        for raw_address, info in dict(self.m.get_slab_stats()).items():
            address = raw_address.split()[0]
            used_memory_value = info['total_malloced']
            self.memory_used.add_metric([address], int(used_memory_value))


        yield self.get_hits
        yield self.get_misses
        yield self.up_time
        yield self.curr_connections
        yield self.total_connections
        yield self.repcached_qi_free
        yield self.memory_used
        yield self.memory_max
        yield self.mem_up
        yield self.cmd_set
        yield self.cmd_get
        yield self.evictions


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="collect the memcached sevrer info")
    parser.add_argument('-a', '--address', type=str, nargs="*", default=["ha1.pg:11211","ha2.pg:11211"],
                        help='memcached address, example: -a ha1.pg:11211 ha2.pg:11211')
    parser.add_argument('-p', '--port', type=int, default=10001,
                        help='the port listen of this exporter')

    args = vars(parser.parse_args())
    addr = args['address']
    port = args['port']
    REGISTRY.register(RepMemcached_Stats(addr))
    print('Start repmemcached exporter listen {0} on port {1}.......'.format(addr, port))
    start_http_server(port)
    while True:
        time.sleep(5)
