import logging
import os
import socket
import sys

from distributed import LocalCluster
from distributed.utils import get_ip_interface

from core import JobQueueCluster

logger = logging.getLogger(__name__)

dirname = os.path.dirname(sys.executable)

class LSFCluster(JobQueueCluster):
	""" Launch Dask on a LSF cluster

	Parameters
	----------
	name : str
		Name of worker jobs. 
	queue : str
		Destination queue for each worker job. 
	threads_per_worker : int
		Number of threads per process.
	processes : int
		Number of processes per node.
	memory : str
		Bytes of memory that the worker can use. LSF and Dask use different annotations for memory usage
	resource_spec : str
		Request resources and specify job placement. Passed to `#LSF -l`
		option.
	walltime : str
		Walltime for each worker job.
	interface : str
		Network interface like 'eth0' or 'ib0'.
	death_timeout : float
		Seconds to wait for a scheduler before closing workers
	extra : str
		Additional arguments to pass to `dask-worker`
	load : str
		Additional arguments to pass to add to bash script template for loading modules, pyenv etc
	kwargs : dict
		Additional keyword arguments to pass to `LocalCluster`

	Examples
	--------
	>>> from dask_jobqueue import LSFCluster
	>>> cluster = LSFCluster()
	>>> cluster.start_workers(10)  # this may take a few seconds to launch

	>>> from dask.distributed import Client
	>>> client = Client(cluster)

	This also works with adaptive clusters.  This automatically launches and
	kill workers based on load.

	>>> cluster.adapt()
	"""

	def __init__(self,
				name='dask-server',
				queue='normal',
				resource_spec={},
				threads_per_worker=4,
				processes=10,
				memory='7000',
				memkill = '7000000',
				walltime='300',
				interface=None,
				death_timeout=60,
				extra='',
				load='pyenv activate myenv',

				**kwargs):

		self._template = """
		#!/bin/bash

		#BSUB -M %(memkill)s
		#BSUB -R %(memrequired)s
		#BSUB -J %(name)s
		#BSUB -q %(queue)s
		#BSUB -W %(walltime)s
		#BSUB -n %(processors)s

		%(load)s

		dask-worker %(scheduler)s \
			--nthreads %(threads_per_worker)d \
			--nprocs %(processors)s \
			--memory-limit %(memory)s \
			--name %(name)s-%(n)d \
			--death-timeout %(death_timeout)s \
			 %(extra)s
		""".lstrip()

		if interface:
			host = get_ip_interface(interface)
			extra += ' --interface  %s ' % interface
		else:
			host = socket.gethostname()

		self.cluster = LocalCluster(n_workers=0, ip=host, **kwargs)

		memory = memory.replace(' ', '')
		
		self.config = {'name': name,
						'queue': queue,
						'project': project,
						'threads_per_worker': threads_per_worker,
						'processes': processes,
						'walltime': walltime,
						'scheduler': self.scheduler.address,
						'resource_spec': resource_spec,
						'base_path': dirname,
						'memory': str(int(memory/1000)) +'GB',
						'death_timeout': death_timeout,
						'extra': extra}
		self.jobs = dict()
		self.n = 0
		self._adaptive = None
		self._submitcmd = 'bsub'
		self._cancelcmd = 'bkill'

		logger.debug("Job script: \n %s" % self.job_script())
