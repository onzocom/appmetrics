# -*- coding: utf-8 -*-

from datetime import datetime as dt
import json
import functools
import threading
import time
import logging
import boto.ec2.cloudwatch as cw
from appmetrics import py3comp, histogram, meter, simple_metrics
from appmetrics.exceptions import DuplicateMetricError, InvalidMetricError

log = logging.getLogger('appmetrics.cloudwatch')


class CloudWatchReporter(object):

    """
    Histogram Output

{"geometric_mean": 0.03325534873580043, "variance": 0.000710514940172709, "kind": "histogram",
 "skewness": -0.18249688345852352, "harmonic_mean": 0.006975782085630464, "min": 0.0002560615539550781,
 "standard_deviation": 0.02665548611773398, "median": 0.05144786834716797,
 "arithmetic_mean": 0.04756087916237967,
 "percentile": [[50, 0.05036592483520508], [75, 0.07094383239746094], [90, 0.07813692092895508],
                [95, 0.08578801155090332], [99, 0.09396910667419434], [99.9, 0.09396910667419434]],
 "max": 0.09396910667419434, "n": 42,
 "histogram": [[1.000256061553955, 42]], "kurtosis": -1.0864722693859035}

Meter Output

{"count": 184, "kind": "meter", "five": 18.786777163057295, "one": 18.73603553170346,
 "fifteen": 18.79556787840392, "day": 18.799953705043272, "mean": 18.384584630563918}

where
    count: number of operations collected so far
    mean: the average throughput since the metric creation
    one: one-minute exponentially-weighted moving average (EWMA)
    five: five-minutes EWMA
    fifteen: fifteen-minutes EWMA
    day: last day EWMA
    kind: "meter"

Counter Output
{'kind': 'counter', 'value': 5}

Gauge Output
{'kind': 'gauge', 'value': 'version 1.0'}

Meter Output
{'count': 5, 'kind': 'meter', 'five': 0.0066114184713530035, 'mean': 0.27743058841197027,
 'fifteen': 0.0022160607980413085, 'day': 2.3147478365093123e-05, 'one': 0.031982234148270686}

Valid Unit values
    Seconds | Microseconds | Milliseconds | Bytes | Kilobytes | Megabytes | Gigabytes |
    Terabytes | Bits | Kilobits | Megabits | Gigabits | Terabits | Percent | Count |
    Bytes/Second | Kilobytes/Second | Megabytes/Second | Gigabytes/Second |
    Terabytes/Second | Bits/Second | Kilobits/Second | Megabits/Second |
    Gigabits/Second | Terabits/Second | Count/Second | None
"""

    def __init__(
            self,
            region,
            namespace,
            dimensions,
            interval=5,
            headers=[],
            **kwargs):
        self._refresh_interval = interval
        self._region = region
        self._namespace = namespace
        self._dimensions = dimensions
        self._headers = headers
        self._conn = cw.connect_to_region(self._region, **kwargs)

    @property
    def refresh_interval(self):
        return self._refresh_interval

    @refresh_interval.setter
    def refresh_interval(self, value):
        self._refresh_interval = value

    @property
    def region(self):
        return self._region

    @property
    def namespace(self):
        return self._namespace

    @property
    def dimensions(self):
        return self._dimensions

    @property
    def headers(self):
        return self._headers

    def dump_histogram(self, name, obj):

        log.debug("dump_histogram:\n{0}".format(json.dumps(obj)))

        # histogram doesn't fit into a tabular format
        obj.pop('histogram')

        # get metric specific dimensions
        dimensions = self.dimensions.copy()
        parts = name.split(':')
        for i in range(len(self.headers)):
            dimensions[self.headers[i]] = parts[i]

        # we already know its kind
        kind = obj.pop('kind')

        # flatten percentiles
        percentiles = obj.pop('percentile')
        for k, v in percentiles:
            obj['percentile_{}'.format(k)] = v

        # add the current time
        timestamp = dt.now()
        unit = "None"

        try:
            self._conn.put_metric_data(self._namespace,
                                       ['.'.join([parts[-1],
                                                  n]) for n in obj.keys()],
                                       value=[v for v in obj.values()],
                                       timestamp=timestamp,
                                       unit=unit,
                                       dimensions=dimensions,
                                       statistics=None)

        except Exception as e:
            log.error("Put Metrics Exception: {}".format(e))

    def dump_meter(self, name, obj):

        log.debug("dump_meter:\n{0}".format(json.dumps(obj)))

        # we already know its kind
        kind = obj.pop('kind')

        # get metric specific dimensions
        dimensions = self.dimensions.copy()
        parts = name.split(':')
        for i in range(len(self.headers)):
            dimensions[self.headers[i]] = parts[i]

        # add the current time
        timestamp = dt.now()
        unit = "None"

        try:
            self._conn.put_metric_data(self._namespace,
                                       ['.'.join([parts[-1],
                                                  n]) for n in obj.keys()],
                                       value=[v for v in obj.values()],
                                       timestamp=timestamp,
                                       unit=unit,
                                       dimensions=dimensions,
                                       statistics=None)

        except Exception as e:
            log.error("Put Metrics Exception: {}".format(e))

    def dump(self, name, obj):

        log.debug("dump_gauge:\n{0}".format(json.dumps(obj)))

        # we already know its kind
        kind = obj.pop('kind')

        # get metric specific dimensions
        dimensions = self.dimensions.copy()
        parts = name.split(':')
        for i in range(len(self.headers)):
            dimensions[self.headers[i]] = parts[i]

        # add the current time
        timestamp = dt.now()
        unit = "None"

        try:
            self._conn.put_metric_data(self._namespace,
                                       ['.'.join([parts[-1],
                                                  n]) for n in obj.keys()],
                                       value=[v for v in obj.values()],
                                       timestamp=timestamp,
                                       unit=unit,
                                       dimensions=dimensions,
                                       statistics=None)

        except Exception as e:
            log.error("Put Metrics Exception: {}".format(e))

    def dump_gauge(self, name, obj):
        self.dump(name, obj)

    def dump_counter(self, name, obj):
        self.dump(name, obj)

    def __call__(self, objects):
        for name, obj in py3comp.iteritems(objects):
            fun = getattr(self, "dump_%s" % obj.get('kind', "unknown"), None)
            if fun:
                # protect the original object
                fun(name, obj.copy())
