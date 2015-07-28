#!/usr/bin/env python
"""Listeners for handling streaming events."""

import logging
import simplejson as json


from base import BaseListener



log = logging.getLogger(__name__)


class PrintingListener(BaseListener):

    """Printing Listener."""

    def __init__(self, api=None):
        """construct printing listener."""
        super(PrintingListener, self).__init__(api)


    def on_status(self, status, raw_data):
        """handle status events."""
        retval = super(PrintingListener, self).on_status(
            self,
            status,
            raw_data)
        print json.dumps(status)

        return retval
