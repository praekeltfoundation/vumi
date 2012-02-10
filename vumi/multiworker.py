# -*- test-case-name: vumi.tests.test_multiworker -*-

from copy import deepcopy

from vumi.service import Worker, WorkerCreator


class MultiWorker(Worker):
    """A worker whose job it is to start other workers.

    Config options:

    :type workers: dict
    :param workers:
        Dict of worker_name -> fully-qualified class name.
    :type defaults: dict
    :param defaults:
        Default configuration for child workers.

    Each entry in the ``workers`` config dict defines a child worker to start.
    A child worker's configuration should be provided in a config dict keyed by
    its name. Common configuration across child workers should go in the
    ``defaults`` config dict.
    """

    WORKER_CREATOR = WorkerCreator

    def construct_worker_config(self, worker_name):
        """
        Construct an appropriate configuration for the child worker.
        """
        config = deepcopy(self.config.get('defaults', {}))
        config.update(self.config.get(worker_name, {}))
        return config

    def create_worker(self, worker_name, worker_class):
        """
        Create a child worker.
        """
        config = self.construct_worker_config(worker_name)
        worker = self.worker_creator.create_worker(worker_class, config)
        worker.setName(worker_name)
        worker.setServiceParent(self)
        return worker

    def startService(self):
        super(MultiWorker, self).startService()
        self.workers = []
        self.worker_creator = self.WORKER_CREATOR(self.options)
        for wname, wclass in self.config.get('workers', {}).items():
            worker = self.create_worker(wname, wclass)
            self.workers.append(worker)

    def startWorker(self):
        pass
