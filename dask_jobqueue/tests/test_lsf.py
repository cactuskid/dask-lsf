import os
from time import time, sleep

import pytest

from distributed import Client
from distributed.utils_test import loop  # noqa: F401
from dask_jobqueue.lsf import LSFCluster

def test_basic(loop):
    with LSFCluster(name='daskTest',
                queue='normal',
                extra='',
                load='pyenv activate myenv'
                ) as cluster:

        with Client(cluster) as client:
            workers = cluster.start_workers(2)
            future = client.submit(lambda x: x + 1, 10)
            assert future.result(60) == 11
            assert cluster.jobs

            info = client.scheduler_info()
            w = list(info['workers'].values())[0]
            assert w['memory_limit'] == 7e9
            assert w['ncores'] == 2

            cluster.stop_workers(workers)

            start = time()
            while len(client.scheduler_info()['workers']) > 0:
                sleep(0.100)
                assert time() < start + 10

            assert not cluster.jobs


def test_adaptive(loop):
    with LSFCluster(name='daskTest',
                queue='normal',
                extra='',
                load='pyenv activate myenv',
                loop=loop) as cluster:
        cluster.adapt()
        with Client(cluster) as client:
            future = client.submit(lambda x: x + 1, 10)
            assert future.result(60) == 11

            assert cluster.jobs

            start = time()
            while len(client.scheduler_info()['workers']) != cluster.config['processes']:
                sleep(0.1)
                assert time() < start + 10

            del future

            start = time()
            while len(client.scheduler_info()['workers']) > 0:
                sleep(0.100)
                assert time() < start + 10

            start = time()
            while cluster.jobs:
                sleep(0.100)
                assert time() < start + 10

