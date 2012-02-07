# -*- test-case-name: vumi.tests.test_multiworker -*-


from twisted.internet.defer import DeferredList, maybeDeferred

from vumi.service import Worker, WorkerCreator


class MultiWorker(Worker):

    WORKER_CREATOR = WorkerCreator

    def startWorker(self):
        self.workers = []
        self.worker_creator = self.WORKER_CREATOR(self.options)
        for wname, wclass in self.config.get('worker_classes', {}).items():
            worker = self.worker_creator.create_worker(wclass, self.config)
            worker.setName(wname)
            worker.setServiceParent(self)

        return DeferredList([maybeDeferred(w.startWorker) for w in self])
