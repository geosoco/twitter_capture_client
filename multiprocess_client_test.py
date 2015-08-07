#!/usr/bin/env python
"""Base classes for clients."""

import argparse
import logging
import time
import logging.config
from multiprocessing import freeze_support
from clients.multiprocess import MultiprocessClientBase
import signal

from configfile import ConfigFile
import functools
import exceptions





#
# configure basic logger
#

logging.basicConfig(
    format='%(asctime)-15s %(processName)s %(levelname)s %(message)s',
    level=logging.DEBUG)


log = logging.getLogger("main")


#
# signal handler
#
def on_signal(client, sig, stack):
    """handle interrupt signaler."""

    global running
    log.info("got interrupt %s", sig)
    running = False
    log.info("set running to false (%s)", running)



if __name__ == "__main__":
    freeze_support()

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


    # create and run client
    try:
        client = MultiprocessClientBase(None, None, config)
        client.run()
    except  exceptions.KeyboardInterrupt, e:
        log.info("keyboard interrupt")
        client.stop_process()
        log.info("stopping")


    # signal_handler = functools.partial(on_signal, client)

    # signal.signal(signal.SIGINT, signal_handler)
    # signal.signal(signal.SIGHUP, signal_handler)
    # signal.signal(signal.SIGQUIT, signal_handler)
    # signal.signal(signal.SIGTERM, signal_handler)
    # signal.signal(signal.SIGKILL, on_interrupt)



    

    log.debug("exiting")
