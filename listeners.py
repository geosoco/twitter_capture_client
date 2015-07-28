#!/usr/bin/env python
"""Listeners for handling streaming events."""

import os
import logging
import simplejson as json
from time import time

from twitter_monitor import JsonStreamListener
from rotating_out_file import RotatingOutFile


log = logging.getLogger(__name__)


class BaseListener(JsonStreamListener):

    """Base listener that implements some counting mechanisms."""

    def __init__(self, api=None):
        """Construct base listener for tweepy."""
        super(BaseListener, self).__init__(api)
        self.terminate = False
        self.received = 0
        self.total = 0
        self.since = time()
        self.rate = 0
        self.connected = False
        log.debug("BaseListener constructed")

    def on_connect(self):
        """handle on_connect event."""
        super(BaseListener, self).on_connect()
        self.connected = True
        return True

    def on_disconnect(self):
        """handle on_disconnect event."""
        super(BaseListener, self).on_disconnect()
        self.connected = False
        return True

    def on_error(self):
        """handle on_error event."""
        super(BaseListener, self).on_error()
        self.connected = False
        return True

    def on_exception(self):
        """handle on_exception event."""
        super(BaseListener, self).on_exception()
        self.connected = False
        return True

    def on_status(self, status):
        """handle on_status event."""
        self.received += 1
        self.total += 1
        return not self.terminate

    def set_terminate(self):
        """Notify the tweepy stream that it should quit."""
        self.terminate = True

    def print_status(self):
        """Log the current tweet rate and reset the counter."""
        tweets = self.received
        now = time()
        diff = now - self.since
        self.since = now
        self.received = 0
        self.rate = (tweets / diff) if diff > 0 else 0
        log.info("Receiving tweets at %s tps", self.rate)



class PrintingListener(BaseListener):

    """Printing Listener."""

    def __init__(self, api=None):
        """construct printing listener."""
        super(PrintingListener, self).__init__(api)


    def on_status(self, status):
        """handle status events."""
        retval = super(PrintingListener, self).on_status(self, status)
        print json.dumps(status)

        return retval



class FileListener(BaseListener):

    """File Listener."""

    def __init__(self, base_dir, collection_name, api=None):
        """Initialize file listener."""
        super(FileListener, self).__init__(api)
        self.base_dir = base_dir
        self.collection_name = collection_name
        self.base_filename = os.join(
            self.base_dir,
            self.collection_name,
            self.collection_name)

    def on_connect(self):
        """handle connect message."""
        super(FileListener, self).on_connect()
        self.file = open(self.base_filename, "a")
        self.connected = True
        return True

    def on_disconnect(self):
        """handle disconnect message."""
        super(FileListener, self).on_disconnect()
        self.disconnected = True
        return True

    def on_status(self, status):
        """handle status message."""
        retval = super(FileListener, self).on_status(status)
        if self.file is not None:
            self.file.write(status + "\n")
        return retval



class RotatingFileListener(BaseListener):

    """Rotating File Listener."""

    def __init__(
            self,
            base_dir=None,
            collection_name=None,
            extension=".json",
            temporary_extension=".tmp",
            minute_interval=10,
            filename_timefmt="%Y%m%d_%H%M",
            api=None):
        """Construct rotating file listener."""
        super(RotatingFileListener, self).__init__(api)
        self.base_dir = base_dir
        self.collection_name = collection_name
        self.file = RotatingOutFile(
            base_dir=self.base_dir,
            collection_name=self.collection_name
        )


    def on_connect(self):
        """handle connect message."""
        retval = super(RotatingFileListener, self).on_connect()
        self.connected = True
        return retval

    def on_disconnect(self):
        """handle disconnect message."""
        retval = super(RotatingFileListener, self).on_disconnect()
        self.disconnected = True
        log.info("RotatingFileListener - disconnect")
        self.file.end_file()
        return retval

    def on_status(self, status):
        """handle status message."""
        self.file.write(json.dumps(status))
        return super(RotatingFileListener, self).on_status(status)
