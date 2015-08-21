#!/usr/bin/env python
"""Base classes for clients."""

import argparse
import logging
import logging.config
from multiprocessing import freeze_support
from clients.multiprocess import MultiprocessClientBase

from configfile import ConfigFile
import exceptions

#
# global
#

log = None


#
# configure basic logger
#

def configure_logging(config=None):
    if config is not None:
        logging.config.dictConfig(config)
    else:
        # set up some default logging options
        logging.basicConfig(
            format=("%(asctime)-15s %(processName) %(levelname)s|"
                    "%(name)s| %(message)s"),
            level=logging.DEBUG
        )


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

    # handle arguments
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
    logging_config = config.getValue(
        "client.logging",
        default=None,
        alternate_paths=["logging"])

    # set up the config
    # import json
    # import pprint
    # print json.dumps(logging_config, indent=4)
    # print "-" * 20

    #pprint.pprint(logging_config, indent=2)
    configure_logging(logging_config)

    # create our log
    log = logging.getLogger("main")


    # create and run client
    try:
        client = MultiprocessClientBase(config)
        client.run()
    except exceptions.KeyboardInterrupt, e:
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
else:
    configure_logging()
