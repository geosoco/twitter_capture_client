#!/usr/bin/env python
"""Base classes for clients."""

import logging
import multiprocessing
import time
from datetime import datetime
import exceptions
import functools

from configfile import ConfigFile
from server_messenger import ServerMessenger, CaptureStatus
from checkers.jobs import JobChecker
from checkers.terms import TermChecker
from listeners.file import RotatingFileListener

from clients.twitter import TwitterClient


class PipeMessenger(object):
    """ Wrapper for pipe communication """


    def __init__(self, pipe, status_callback=None, update_callback=None):
        """ initialize the messenger """
        self.pipe = pipe
        self.status_callback = status_callback
        self.update_callback = update_callback
        self.log = logging.getLogger(self.__class__.__name__)

    def sendStatus(self, status):
        """ sends status """
        self.pipe.send({
            "type": "status",
            "data": status
        })

    def sendUpdate(self, time, received, rate, total):
        """ sends an update """
        self.pipe.send({
            "type": "update",
            "data": {
                "time": time,
                "received": received,
                "rate": rate,
                "total": total
            }
        })

    def receive(self):
        """ receive messages """
        if not self.pipe.poll():
            return False

        # get the message
        msg = self.pipe.recv()

        if msg is None:
            self.log.error("pipe message is None")
            return False

        self.dispatch_message(msg)

        return True

    def dispatch_message(self, msg):
        """ dispatch message to callback """

        msg_type = msg.get("type", None)
        if msg_type is None:
            self.log.error("pipe message has no type")
            return

        if msg_type == "status":
            if self.status_callback is not None:
                self.status_callback(msg["data"])
            else:
                self.log.debug("status message but no callback")
        elif msg_type == "update":
            if self.update_callback is not None:
                self.update_callback(msg["data"])
            else:
                self.log.debug("update message but no callback")



class ClientWorker(object):

    """test..."""

    def __init__(
            self, collection_name, terms, event, pipe, total, config_data):
        self.log = logging.getLogger(self.__class__.__name__)
        self.collection_name = collection_name
        self.event = event
        self.terms = terms
        self.raw_pipe = pipe
        self.config_data = config_data
        self.listener = None
        self.client = None
        self.config = ConfigFile(config_data=self.config_data)
        self.initial_total = total if total is not None else 0

        self.pipe = PipeMessenger(self.raw_pipe)

        self.client_status = CaptureStatus(CaptureStatus.STATUS_UNKNOWN)

    def initialize(self):
        """initialize the worker."""

        output_config = self.config.getValue("output", {})

        self.listener = RotatingFileListener(
            collection_name=self.collection_name,
            **output_config
        )
        self.listener.data_callback = functools.partial(
            self.dataCallback,
            (self,)
        )
        self.listener.stats.total = self.initial_total

        # create client
        self.client = TwitterClient(
            listener=self.listener,
            keywords=self.terms
        )
        self.client.configure(self.config)

        # last stat update
        self.last_stat_update_time = datetime.now()


    def updateStatus(self, status):
        if status != self.client_status.value:
            self.client_status.value = status
            self.pipe.sendStatus(status)


    def shutdown(self):
        """shutdown the worker."""
        self.updateStatus(CaptureStatus.STATUS_STOPPING)

        try:
            self.client.shutdown()
        except Exception:
            self.log.exception("Exception during client shutdown")

        try:
            self.listener.shutdown()
        except Exception:
            self.log.exception("Exception while shutting down listener")

        self.updateStatus(CaptureStatus.STATUS_STOPPED)

    def run(self):
        """run the collection."""
        self.updateStatus(CaptureStatus.STATUS_STARTING)

        self.client.initialize()

        self.client.run()

        self.log.debug("run returned. ClientStatus -> Stopped")
        # self.updateStatus(CaptureStatus.STATUS_STOPPED)


    def dataCallback(self, listener, stats):
        """handle data callback by sending periodic updates."""
        # self.log.debug("worker callback!")
        # self.log.debug("self: %s", repr(self))
        # self.log.debug("listener: %s", repr(listener))
        # self.log.debug("stats: %s", repr(stats))

        # quitting?
        if self.event is None or self.event.is_set():
            return False

        # update status if necessary
        self.updateStatus(CaptureStatus.STATUS_STARTED)

        now = datetime.now()
        delta = now - self.last_stat_update_time
        if delta.total_seconds() > 5:
            received = stats.received
            stats.calculate_rate()
            self.pipe.sendUpdate(now, received, stats.rate, stats.total)
            self.last_stat_update_time = now

        return True


#
# worker thread function
#


def process_worker(
        args, collection_name, event, terms, pipe,
        total, config_data):

    client = None
    try:
        log = logging.getLogger("collection_process")

        config = ConfigFile(config_data=config_data)

        log.info(
            "Starting %s (%s,%s)",
            collection_name,
            repr(event),
            repr(config))

        # create and initialize the client
        client = ClientWorker(
            collection_name, terms, event, pipe, total, config_data)
        client.initialize()

        # run the client
        client.run()
        # profiling code below, first uncomment line above:
        # cProfile.runctx(
        #    "client.run()",
        #    globals(),
        #    locals(),
        #    "client_run.prof")


    except exceptions.KeyboardInterrupt:
        log.warn("--keyboard interrupt")
    finally:
        # tear things down
        if client is not None:
            client.shutdown()

    log.info("PROCESS ENDING")

    # close the queue
    pipe.close()



class MultiprocessClientBase(object):

    """Base class for a multi-processing task"""

    def __init__(self, config):
        """initialize the multiprocess client"""
        self.client = None
        self.listener = None
        self.config = config
        self.log = logging.getLogger(self.__class__.__name__)
        self.event = multiprocessing.Event()
        self.process = None
        self.messenger = None
        self.shutting_down = False

        self.create_server_messenger()
        self.job_checker = JobChecker(self.messenger)

        self.client_pipe, self.worker_pipe = multiprocessing.Pipe()
        self.pipe = PipeMessenger(
            self.client_pipe,
            status_callback=self.on_status,
            update_callback=self.on_update)

        self.client_status = CaptureStatus(CaptureStatus.STATUS_UNKNOWN)

    def create_server_messenger(self):
        """create the server messenger object"""
        base_url = self.config.getValue("server.base_url", None)
        auth_token = self.config.getValue("server.auth_token", None)
        self.messenger = ServerMessenger(
            base_url,
            auth_token)

    def build_client(self):
        """build, initialize, and configure the client."""
        pass

    def start_process(
            self, collection_name, initial_terms=None, initial_total=0):
        """start the separate capture process"""
        self.log.debug("starting...")

        # reset event
        self.event.clear()

        children = multiprocessing.active_children()
        if children is not None and len(children) > 0:
            self.log.debug("%d active children", len(children))
            return

        self.process = multiprocessing.Process(
            target=process_worker,
            args=(self,),
            kwargs={
                "collection_name": collection_name,
                "event": self.event,
                "terms": initial_terms,
                "pipe": self.worker_pipe,
                "total": initial_total,
                "config_data": self.config.config_data
            })
        # make it a daemon
        self.process.daemon = True

        # go!
        self.process.start()


    def stop_process(self):
        """stop the capture process"""
        self.event.set()
        self.wait_for_child()

    def on_status(self, status):
        """receive status update"""
        self.client_status = CaptureStatus(status)

    def on_update(self, data):
        """receive update message"""
        self.log.debug("on update %s", repr(data))
        # self.messenger.pingServer(
        #    data["total"],
        #    data["rate"])
        self.messenger.putUpdate(
            data["received"],
            data["total"],
            data["rate"])


    def wait_for_child(self):
        """wait for child"""
        if self.process is not None:
            while self.process.is_alive():
                time.sleep(0.5)

            # join to prevent zombie
            self.process.join()

            # update client status
            # self.messenger.updateStatus(CaptureStatus.STATUS_STOPPED)


    def run(self):
        """run capture"""
        self.log.debug("starting client")

        while not self.shutting_down:
            self.wait_for_job()

            if (not self.shutting_down and
                    self.job_checker.activeJob is not None):
                self.log.info("wait for active job -> starting collection")

                self.do_job()

                self.log.info("stopping collection -> waiting for active job")

        self.log.info("exiting run")


    def wait_for_job(self):
        """waits for an active job from the server"""
        # deactivate any job
        self.job_checker.deactivateJob()
        active_job = None

        # progress while we're not
        while not self.shutting_down:
            # grab the first active job
            active_job = self.job_checker.getFirstActiveJob()
            if active_job is not None:
                status = CaptureStatus(active_job["status"])
                if status.running:
                    self.job_checker.activateJob(active_job)
                    break
                else:
                    self.log.debug("active job is stopped")
            else:
                self.log.debug("no active job")

            # pause for a while
            time.sleep(5)


    def handle_message(self, msg):
        """ handle a message from the process """
        pass


    def do_job(self):
        self.log.debug("starting collection")

        # create term checker
        term_checker = TermChecker(self.messenger)
        term_checker.checkTerms()

        # reset event
        self.event.clear()

        # start process
        # self.start_process(term_checker.terms)

        # restart flag
        restart_pending = False

        while not self.shutting_down and not self.event.is_set():

            # wait loop, check messages during this
            time_before = datetime.now()
            now = datetime.now()
            while (now - time_before).total_seconds() < 5:

                # handle messages or sleep
                received = self.pipe.receive()
                if not received:
                    time.sleep(1)

                # update time
                now = datetime.now()

            # check status
            job_status = self.messenger.getStatus()
            if job_status is None:
                continue

            status = CaptureStatus(job_status["status"])

            # recheck terms
            term_checker.checkTerms()
            if term_checker.haveTermsChanged():
                self.log.info("terms changed...")
                restart_pending = True
                term_checker.resetTermsChanged()

            # attempt to reconcile the two statuses
            new_status = None
            if status != self.client_status:
                self.log.info(
                    "different statuses (server:%s, client:%s)",
                    status,
                    self.client_status)


                if status.stopped:
                    if self.client_status.running:
                        self.log.info("stopping capture")
                        self.stop_process()
                        if status.value == CaptureStatus.STATUS_STOPPING:
                            new_status = CaptureStatus(
                                CaptureStatus.STATUS_STOPPED)
                elif status.running:
                    if self.client_status == CaptureStatus.STATUS_STARTED:
                        new_status = self.client_status
                    else:
                        self.start_process(
                            job_status["name"],
                            term_checker.terms,
                            job_status["total_count"])
                        restart_pending = False

            # update server status
            if new_status is not None:
                self.log.debug("changing status to %s", new_status)
                self.messenger.updateStatus(new_status.value)


            # check for restart
            if restart_pending:
                self.log.info("restarting...")
                self.stop_process()
                time.sleep(10)

                term_checker.checkTerms()
                self.start_process(
                    job_status["name"], 
                    term_checker.terms,
                    job_status["total_count"])
                term_checker.resetTermsChanged()

                restart_pending = False

        # quitting
        self.log.debug(
            "quitting job (%s,%s)",
            repr(self.shutting_down),
            repr(self.event.is_set()))
