
from __future__ import print_function

import logging
import os
import sys
import random
import time

import boto.ec2.cloudwatch as cw

from appmetrics import cloudwatch, metrics, reporter


logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.DEBUG)


@metrics.with_meter('cloudwatch_reporter:worker_meter')
def worker():
    # just spend some time
    with metrics.this_histogram('cloudwatch_reporter:worker_histogram'):
        with metrics.timer('cloudwatch_reporter:worker_timer'):
            time.sleep(random.random() / 10.0)


def main(region,
         namespace,
         aws_access_key_id=None,
         aws_secret_access_key=None):

    if not (namespace and region):
        sys.exit('ERROR: You must supply a namespace and a region')

    if ((aws_access_key_id and not aws_secret_access_key) or
            (aws_secret_access_key and not aws_access_key_id)):
        sys.exit(
            'ERROR: Supply both aws_access_key_id and '
            'aws_secret_access_key, or supply neither')

    # Register a CloudWatch reporter that will dump the metrics with
    # the tag 'worker' each 30 seconds to csv files in the given directory
    if aws_access_key_id:
        cloudwatch_reporter = cloudwatch.CloudWatchReporter(
            region,
            namespace,
            {'server': 'test'},
            headers=['app'],
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key)
        reporter.register(cloudwatch_reporter,
                          reporter.fixed_interval_scheduler(30))
    else:
        reporter.register(
            cloudwatch.CloudWatchReporter(region, namespace, 'server=test'),
            reporter.fixed_interval_scheduler(30))

    # emulate some work
    print('Hit CTRL-C to stop the process')
    while True:
        try:
            worker()
        except KeyboardInterrupt:
            break


if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit(
            'Usage: {} <region> <namespace> [ <aws_access_key> '
            '<aws_secret_key> ]'.format(sys.argv[0]))

    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
