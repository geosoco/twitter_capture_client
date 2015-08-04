#!/usr/bin/env python
"""Time out checkers."""

from datetime import datetime


class TimeOutCheck(object):

    """Class to manage time outs."""

    def __init__(self, timeout):
        """initialize the teimout check"""
        self.last_time = None
        self.timeout = timeout
        self.callback = None

    def isTimeOut(self):
        """return True if time is up."""
        return False

    def check(self):
        """check if timeout has occurred.

        returns value of timeout check and calls callback if necessary
        and resets the timeout."""
        if self.isTimeOut():
            if self.callback is not None:
                self.callback()
            self.reset()

    def reset(self):
        """reset the timeout."""
        self.last_time = datetime.now()
