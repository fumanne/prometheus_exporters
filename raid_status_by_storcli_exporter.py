#! /usr/local/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function
from prometheus_client import start_http_server
from prometheus_client.core import REGISTRY, GaugeMetricFamily
import argparse
import os
import json
import subprocess
import time


class RaidSatus(object):
    def __init__(self, path):
        self.prefix = "RaidStatus"
        self.path = path
        if not os.path.isfile(self.path):
            raise ValueError("{} is not exists".format(self.path))

        if not os.access(self.path, os.X_OK):
            raise SystemError("{} have no permission to access".format(self.path))

        self.data = json.loads(self._fetch_data())
        self.system_overview = self.data['Controllers'][0]['Response Data']['System Overview'][0]
        self.metrics = self._generate_metrics(self.system_overview)

    def _fetch_data(self):
        _cmd = [self.path, 'show', 'all', 'J']
        proc = subprocess.Popen(_cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return proc.communicate()[0]

    def _generate_metrics(self, overview):
        if not isinstance(overview, dict):
            raise ValueError('{} is not dict type'.format(overview))
        metrics = {
                        "_".join([self.prefix, "System_Overview_Ports"]): overview['Ports'],
                        "_".join([self.prefix, "System_Overview_PDs"]): overview['PDs'],
                        "_".join([self.prefix, "System_Overview_DGs"]): overview['DGs'],
                        "_".join([self.prefix, "System_Overview_DNOpt"]): int(overview['DNOpt'] == 0),
                        "_".join([self.prefix, "System_Overview_VDs"]): overview['VDs'],
                        "_".join([self.prefix, "System_Overview_VNOpt"]): int(overview['VNOpt'] == 0),
                        "_".join([self.prefix, "System_Overview_BBU"]): int(overview['BBU'] == "Opt"),
                        "_".join([self.prefix, "System_Overview_sPR"]): int(overview['sPR'] == "On"),
                        "_".join([self.prefix, "System_Overview_DS"]): int(overview['DS']) if overview['DS'].isdigit() else 0,
                        "_".join([self.prefix, "System_Overview_EHS"]): 1 if overview['EHS'] == "Y" else 0,
                        "_".join([self.prefix, "System_Overview_ASOs"]): overview['ASOs'],
                        "_".join([self.prefix, "System_Overview_Hlth"]): int(overview['Hlth'] == 'Opt')
                    }
        return metrics


    def collect(self):
        for k, v in self.metrics.items():
            guage_metrics = GaugeMetricFamily(k, k.replace('_', ' '), labels=['Controller_Index', 'Model'])
            guage_metrics.add_metric([str(self.system_overview['Ctl']), self.system_overview['Model']], v)
            yield guage_metrics

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parses StorCLI's JSON output and exposes MegaRAID health as Prometheus metrics")
    parser.add_argument('-p', '--path', type=str, default='/opt/MegaRAID/storcli/storcli64')
    parser.add_argument('-P', '--port', type=int, default=10003)
    args = vars(parser.parse_args())
    path = args['path']
    port = args['port']
    REGISTRY.register(RaidSatus(path))
    print("Start Monitor MegaRAID Health exporter on listen {} ....... ".format(port))
    start_http_server(port)
    while True:
        time.sleep(30)
