from copy import deepcopy

from vumi.service import Worker, WorkerCreator


class WorkerLoaderBase(Worker):
    def load_worker(self, worker_class, config, vumi_options_override=None):
        options = deepcopy(self.vumi_options)
        if vumi_options_override:
            options.update(vumi_options_override)
        creator = WorkerCreator(options)
        creator.create_worker(worker_class, config)

