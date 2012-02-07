# -*- test-case-name: vumi.tests.test_multiworker -*-

from copy import deepcopy

from twisted.internet.defer import DeferredList, maybeDeferred

from vumi.service import Worker, WorkerCreator


class MultiWorker(Worker):
    """A worker whose job it is to start other workers.

    Config options:

    :type worker_classes: dict
    :param worker_classes:
        Dict of worker_name -> fully-qualified class name.
    :type worker_config_extras: dict
    :param worker_config_extras:
        Dict of worker_name -> worker-specific config dict.

    All workers specified in the ``worker_classes`` config option are created
    and started in :meth:`startWorker` and set as child services. They are
    given config dicts that are copies of the :class:`MultiWorker`'s config
    modified in the following way:

     * Options specific to :class:`MultiWorker` are removed.

     * Items in the ``worker_config_extras`` entry for the given worker name
       are moved to the top level of the config.
    """

    WORKER_CREATOR = WorkerCreator

    def construct_worker_config(self, worker_name):
        """
        Construct an appropriate configuration for the child worker.
        """
        config = deepcopy(self.config)
        config.setdefault('worker_config_extras', {})
        config.update(config['worker_config_extras'].get(worker_name, {}))
        config.pop('worker_classes')
        config.pop('worker_config_extras')
        return config

    def create_worker(self, worker_name, worker_class):
        """
        Create a child worker.
        """
        config = self.construct_worker_config(worker_name)
        worker = self.worker_creator.create_worker(worker_class, config)
        worker.setName(worker_name)
        worker.setServiceParent(self)

    def startWorker(self):
        self.workers = []
        self.worker_creator = self.WORKER_CREATOR(self.options)
        for wname, wclass in self.config.get('worker_classes', {}).items():
            self.create_worker(wname, wclass)

        return DeferredList([maybeDeferred(w.startWorker) for w in self])
