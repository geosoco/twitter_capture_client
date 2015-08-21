#!/usr/bin/env python
"""Base Listener for handling streaming events."""


import logging
from time import time
from tweepy.streaming import StreamListener
import simplejson as json


log = logging.getLogger(__name__)


class ListenerStats(object):

    """Stats class for the listener."""

    def __init__(self, initial_total=0):
        """initialize the stats counter."""
        self.received = 0
        self.total = initial_total
        self.since = time()
        self.rate = 0

    def increment(self, amount=1):
        """increment the current received and total counts."""
        self.received += amount
        self.total += amount

    def calculate_rate(self):
        """calculate the current rate and reset counter."""
        now = time()
        diff = now - self.since
        # if there's a difference, calculate it
        if diff > 0:
            self.since = now
            self.rate = (self.received / diff) if diff > 0 else 0
            self.received = 0

    def __str__(self):
        return "Total: %d (Rate %s)" % (self.total, self.rate)


class BaseListener(StreamListener):

    """Base listener that implements some counting mechanisms."""

    def __init__(self, api=None):
        """Construct base listener for tweepy."""
        super(BaseListener, self).__init__(api)
        self.terminate = False
        self.connected = False
        self.error = False
        self.stats = ListenerStats(0)
        self.data_callback = None
        log.debug("BaseListener constructed")

    def shutdown(self):
        """shutdown the listener."""
        pass

    def on_connect(self):
        """handle on_connect event."""
        super(BaseListener, self).on_connect()
        self.connected = True
        return True

    def on_disconnect(self, notice):
        """handle on_disconnect event."""
        super(BaseListener, self).on_disconnect()
        self.connected = False
        log.info("stream disconnect: ", notice)
        return True

    def on_error(self, status_code):
        """handle on_error event."""
        super(BaseListener, self, status_code).on_error()
        self.connected = False
        self.error = True
        log.error("stream error: %s", status_code)
        return True

    def on_exception(self, exception):
        """handle on_exception event."""
        super(BaseListener, self).on_exception(exception)
        self.connected = False
        self.error = True
        log.exception("exception: %s", repr(exception))
        return True


    def on_data(self, raw_data):
        """handle raw status data

        Override the default the default handler to not use their
        Status class.
        """

        data = json.loads(raw_data)

        if 'in_reply_to_status_id' in data:
            if self.on_status(data, raw_data) is False:
                return False
        elif 'delete' in data:
            delete = data['delete']['status']
            if self.on_delete(
                    delete['id'],
                    delete['user_id'],
                    data,
                    raw_data) is False:
                return False
        elif 'event' in data:
            if self.on_event(data, raw_data) is False:
                return False
        elif 'direct_message' in data:
            if self.on_direct_message(data, raw_data) is False:
                return False
        elif 'friends' in data:
            if self.on_friends(data['friends'], data, raw_data) is False:
                return False
        elif 'limit' in data:
            if self.on_limit(
                    data['limit']['track'],
                    data,
                    raw_data) is False:
                return False
        elif 'disconnect' in data:
            if self.on_disconnect(data['disconnect']) is False:
                return False
        elif 'warning' in data:
            if self.on_warning(data['warning'], data, raw_data) is False:
                return False
        else:
            log.error("Unknown message type: " + str(raw_data))

        if self.error or not self.connected:
            log.info(
                "listener returning false (%s,%s)",
                self.error,
                self.connected)
            return False

        if self.data_callback:
            return self.data_callback(self.stats)

        return True


    def on_status(self, status, raw_data):
        """handle on_status event."""
        self.stats.increment()
        return not self.terminate


    def on_delete(self, id, user_id, data, raw_data):
        """handle delete message"""
        return not self.terminate

    def on_event(self, data, raw_data):
        """handle stream event message"""
        return not self.terminate

    def on_direct_message(self, data, raw_data):
        """handle direct message"""
        return not self.terminate

    def on_friends(self, friends, data, raw_data):
        """handle friends message"""
        return not self.terminate

    def on_limit(self, limit, data, raw_data):
        """handle limited data"""
        log.warn("limit for %s", limit)
        log.debug("limit: %s", repr(data))
        return not self.terminate

    def on_warning(self, warning, data, raw_data):
        """handle warning message"""
        log.warn("stream warning: %s", warning)
        return not self.terminate

    def on_timeout(self):
        log.error("timeout occurred")
        self.connected = False
        self.error = True
        return not self.terminate

    def set_terminate(self):
        """Notify the tweepy stream that it should quit."""
        self.terminate = True

    def print_status(self):
        """Log the current tweet rate and reset the counter."""
        self.stats.calculate_rate()
        log.info("Receiving tweets at %s tps", str(self.stats))
