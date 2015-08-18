#!/usr/bin/env python
"""Base classes for clients."""

import logging


class JobChecker(object):

    """Wrapper class for checking jobs"""


    def __init__(self, server_messenger):
        self.server_messenger = server_messenger
        self.active_job = None
        self.active_job_id = None
        self.log = logging.getLogger(self.__class__.__name__)


    def configure(self, config):
        pass

    def requestFirstActiveJob(self):
        active_jobs = self.server_messenger.getActiveJobs()

        # if we got active jobs back...
        if active_jobs is not None:
            count = active_jobs["count"]

            # if we get results
            if count > 0:
                # we should never get more than one, so warn
                if count > 1:
                    self.log.warn(
                        "more than one active_job. got %d for id %d",
                        count,
                        self.client_id
                    )
                    self.log.warn(active_jobs)

                return active_jobs["results"][0]
        # mass failure
        return None


    def getFirstActiveJob(self):
        return self.requestFirstActiveJob()

    def activateJob(self, job):
        if job is None:
            self.log.error("attempt to activate job that is None.")
            return

        self.active_job = job
        self.active_job_id = job["id"]
        self.server_messenger.active_job_id = self.active_job_id

    def deactivateJob(self):
        self.active_job = None
        self.active_job_id = None
        self.server_messenger.active_job_id = None

    def getActiveJobId(self):
        return self.active_job_id

    @property
    def activeJob(self):
        return self.active_job
