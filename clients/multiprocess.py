#!/usr/bin/env python
"""Base classes for clients."""

import logging
import multiprocessing
import time
import exceptions


def process_worker(args, name, event, client, listener, config):
    try:
        log = logging.getLogger(name)

        log.info(
            "Starging %s (%s,%s,%s,%s)",
            name,
            repr(event),
            client,
            listener,
            repr(config))
        while not event.is_set():
            time.sleep(1)
            print "ping"
    except exceptions.KeyboardInterrupt:
        log.warn("--keyboard interrupt")
    log.info("PROCESS ENDING")



class MultiprocessClientBase(object):

    """Base class for a multi-processing task"""

    def __init__(self, client, listener, config):
        """initialize the multiprocess client"""
        self.client = client
        self.listener = listener
        self.config = config
        self.log = logging.getLogger(self.__class__.__name__)
        self.process = None
        self.event = multiprocessing.Event()

    def start_process(self):
        """start the separate capture process"""
        self.log.debug("")

        self.process = multiprocessing.Process(
            target=process_worker,
            args=(self,),
            kwargs={
                "name": "test_proc",
                "event": self.event,
                "client": None,
                "listener": None,
                "config": None
            })
        self.process.start()


    def stop_process(self):
        """stop the capture process"""
        self.event.set()
        self.wait_for_child()


    def wait_for_child(self):
        """wait for child"""
        while self.process is not None and self.process.is_alive():
            time.sleep(0.25)


    def run(self):
        """run capture"""
        self.log.debug("starting client")
        self.start_process()

        self.log.debug("sleeping")
        time.sleep(10)

        self.log.debug("stopping client")
        self.stop_process()
