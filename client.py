#!/usr/bin/env python
"""Twitter capture client code."""

import argparse
import logging
import logging.config
from time import sleep
from datetime import datetime


import signal

from listeners.file import RotatingFileListener
from configfile import ConfigFile
from server_messenger import ServerMessenger
from streamer import Streamer



VERSION = 0.1



STATUS_UNKNOWN = 0
STATUS_CREATED = 1
STATUS_STARTING = 2
STATUS_STARTED = 3
STATUS_STOPPING = 4
STATUS_STOPPED = 5
STATUS_UNRESPONSIVE = 6
STATUS_DEAD = 7




log = logging.getLogger(__name__)



running = True




def on_interrupt(sig, stack):
    """handle interrupt signaler."""

    global running
    log.info("got interrupt %s", sig)
    running = False
    log.info("set running to false (%s)", running)



class Client(object):

    """Basic Client class for polling server for jobs."""

    def __init__(
            self,
            base_url,
            token,
            ping_interval,
            update_interval,
            twitter_auth,
            output_config,
            source_addr=None):
        """construct client."""

        self.base_url = base_url
        self.token = token
        self.client_id = client_id
        self.ping_interval = ping_interval
        self.update_interval = update_interval
        self.twitter_auth = twitter_auth
        self.output_config = output_config
        self.source_addr = source_addr
        self.active_job = None
        self.active_job_id = None
        self.log = logging.getLogger("Client")

        self.sm = ServerMessenger(
            base_url=base_url,
            token=token
        )


    def wait_for_job(self):
        """poll for an active job assignment."""

        while running is True:
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

                    # grab the active job details
                    self.active_job = active_jobs["results"][0]
                    self.active_job_id = self.active_job["id"]

                    # return
                    return

            # sleep
            sleep(self.ping_interval)


    def start_collection(self):
        """Start collection."""
        # create our streamer
        collection_name = self.active_job["name"]
        self.listener = RotatingFileListener(
            collection_name=collection_name,
            **self.output_config
        )

        self.stream = Streamer(
            listener=self.listener,
            api_key=self.twitter_auth["api_key"],
            api_secret=self.twitter_auth["api_secret"],
            access_token=self.twitter_auth["access_token"],
            access_token_secret=self.twitter_auth["access_token_secret"],
            source_addr=self.source_addr
        )

        # set job id in server messenger
        self.sm.active_job_id = self.active_job_id


    def run_collection(self):
        """Run collection."""

        # start the collection
        self.start_collection()

        # make sure we got a valid stream
        if self.stream is None:
            log.error("stream was not started")
            return

        # old state defaults
        old_status = STATUS_UNKNOWN

        # initialize keyword details
        current_keywords = set()
        keywords_changed = False

        # we haven't updated yet
        last_update = None

        # archived
        archived_date = None

        # run while valid
        while running is True and archived_date is None:

            # get job status from server
            status_msg = self.sm.getStatus()

            # if we got null, there's a problem with the server,
            # sleep and continue
            if status_msg is None:
                sleep(update_interval)
                continue

            # set up the status
            status = (status_msg['status'] if 'status' in status_msg
                      else STATUS_UNKNOWN)
            keywords = ([kw.strip() for kw in
                        status_msg['twitter_keywords'].split(",")]
                        if 'twitter_keywords' in status_msg else [])

            self.log.debug("got status: %d", status_msg['status'])

            # look for archived date
            archived_date = status_msg["archived_date"]
            if archived_date is not None:
                continue


            # are there any keyword changes?
            keywords_set = set(keywords)
            if keywords_set != current_keywords:
                self.log.info("twitter filter words changed: ")
                subtractions = current_keywords - keywords_set
                additions = keywords_set - current_keywords
                self.log.info("    + : %s", repr(additions))
                self.log.info("    - : %s", repr(subtractions))

                keywords_changed = True
                current_keywords = keywords_set


            if old_status != status:
                self.log.info("changing status#1 %d -> %d", old_status, status)
                if status == STATUS_STARTING or status == STATUS_STARTED:
                    if not self.stream.isRunning():
                        self.log.info("Starting stream")
                        self.stream.track_list = keywords
                        sm_total_count = status_msg['total_count']
                        if sm_total_count is not None:
                            self.listener.total = sm_total_count
                        self.stream.start()
                        # ackknowledge that we have the newest keywords in here
                        keywords_changed = False
                elif status == STATUS_STOPPING or status == STATUS_STOPPED:
                    if self.stream.isRunning():
                        self.log.info("Stopping stream")
                        self.stream.stop()
            elif keywords_changed:
                self.stream.track_list = keywords
                if self.stream.isRunning():
                    self.log.debug("restarting streams for keywords")
                    self.stream.stop()
                    sleep(ping_interval)
                    self.stream.start()
                    keywords_changed = False


            # sleep
            sleep(self.ping_interval)

            # new status
            new_status = STATUS_UNKNOWN
            if self.stream.isRunning():
                self.log.debug("stream exists and is running")
                if status != STATUS_STOPPING:
                    new_status = STATUS_STARTED
            else:
                if status != STATUS_STARTING:
                    self.log.debug(
                        "stream exists but is not running (forcing %d -> %d",
                        status,
                        new_status)
                    new_status = STATUS_STOPPED



            # if there's a discrepancy
            if new_status != status and new_status != STATUS_UNKNOWN:
                self.log.info("changing status#2 %d -> %d", status, new_status)
                self.sm.updateStatus(new_status)

            # update the old status
            old_status = new_status

            # self.log.debug("running  - %s", running)

            # output status
            # send update status to server if we're running
            if self.stream.isRunning():
                self.listener.print_status()
                # sm.putUpdate(
                #   listener.received,
                #   listener.total,
                #   listener.rate
                #  )
                self.sm.pingServer(self.listener.total, self.listener.rate)

                do_update = False
                if last_update is None:
                    self.log.debug("initial update")
                    do_update = True
                else:
                    delta = datetime.now() - last_update
                    if delta.total_seconds() > self.update_interval:
                        self.log.debug(
                            "update delta: %f",
                            delta.total_seconds())
                        do_update = True
                    # else:
                        # self.log.debug("delta: %f", delta.total_seconds())

                # update to server
                if do_update is True:
                    self.sm.putUpdate(
                        self.listener.received,
                        self.listener.total,
                        self.listener.rate
                    )
                    last_update = datetime.now()


            else:
                self.log.debug("waiting for update")


        # wait for stream to stop
        if self.stream.isRunning():
            self.log.info("Stopping...")
            self.stream.stop()

            while self.stream.isRunning():
                self.log.info("Waiting for self.logger to stop")
                sleep(1)

        # allow our listener and stream to be deleted
        self.stream = None
        self.listener = None






    def run(self):
        """Start up the client running machine."""

        while running is True:
            # wait for an active job
            self.wait_for_job()

            if self.active_job is not None:
                self.log.info(
                    "working on job (id: %d, name: %s)",
                    self.active_job_id,
                    self.active_job["name"]
                )

                # start collection
                self.run_collection()
            else:
                sleep(self.ping_interval)










if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--configfile",
        default="config.json",
        help="config file to use"
    )
    args = parser.parse_args()

    # config file
    config = ConfigFile(args.configfile)


    # configure the logging
    if "logging" in config:
        logging.config.dictConfig(config["logging"])
    else:
        # set up some default logging options
        logging.basicConfig(
            format="%(asctime)s|%(levelname)s|%(name)s - %(message)s",
            level=logging.DEBUG
        )
        logging.getLogger("twitter_monitor.listener").setLevel(
            logging.getLevelName('WARN')
        )


    # extract new args
    base_url = config.getValue("server.base_url", None)
    auth_token = config.getValue("server.auth_token", None)
    client_id = config.getValue("server.client_id", None)
    ping_interval = config.getValue("server.ping_interval", 5.0)
    update_interval = config.getValue("server.update_interval", 60.0)
    twitter_auth = config.getValue("twitter_auth", None)
    output_conf = config.getValue("output", None)
    source_addr = config.getValue("source_addr", None)

    print "ta", twitter_auth

    try:
        if not base_url:
            raise Exception("base_url")
        if not auth_token:
            raise Exception("auth_token")
        if not client_id:
            raise Exception("id")
        if not twitter_auth:
            raise Exception("twitter_auth")
        if not output_conf:
            raise Exception("output")
        if "base_dir" not in output_conf:
            raise Exception("output.base_dir")

    except Exception, e:
        msg = "%s was not specified in the config file" % (e.message)
        log.error(msg)
        quit()


    # set signal handlers
    signal.signal(signal.SIGINT, on_interrupt)
    signal.signal(signal.SIGHUP, on_interrupt)
    signal.signal(signal.SIGQUIT, on_interrupt)
    # signal.signal(signal.SIGKILL, on_interrupt)
    signal.signal(signal.SIGTERM, on_interrupt)

    print client_id
    print type(client_id)

    # create the server messenger

    client = Client(
        base_url,
        auth_token,
        ping_interval,
        update_interval,
        twitter_auth,
        output_conf)

    client.run()

    quit()
