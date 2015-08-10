#!/usr/bin/env python
"""Base classes for clients."""

import logging


class JobChecker(object):

    """Wrapper class for checking jobs"""


    def __init__(self, server_manager):
        self.server_manager = server_manager
        self.active_job = None
        self.active_job_id = None
        self.log = logging.getLogger(self.__class__.__name__)


    def configure(self, config):
        pass

    def requestActiveJob(self):
        active_jobs = self.sm.getActiveJobs()

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


    def getActiveJob(self):
        return self.requestActiveJob()
