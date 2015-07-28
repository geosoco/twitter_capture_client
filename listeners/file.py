#!/usr/bin/env python
"""Listeners for handling streaming events."""

import os
import logging
import simplejson as json

from .base import BaseListener
from rotating_out_file import RotatingOutFile


log = logging.getLogger(__name__)


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

    def on_status(self, status, raw_data):
        """handle status message."""
        retval = super(FileListener, self).on_status(status, raw_data)
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

    def on_status(self, status, raw_data):
        """handle status message."""
        # print repr(status)
        # print "\n"*4
        self.file.write(raw_data)
        return super(RotatingFileListener, self).on_status(status, raw_data)
