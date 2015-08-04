#!/usr/bin/env python
"""cached decorators."""

import threading
from datetime import datetime


class cached_function_ttl(object):

    """cached function wrapper"""

    def __init__(self, ttl=None):
        """initialize function decorator.

        ttl = timedelta for ttl
        """
        self.last_call = None
        self.lock = threading.RLock()
        self.ttl = ttl
        self.cached_val = None


    def __call__(self, fn, *args, **kwargs):
        """wrapper for function call"""
        def wrapped_func(*args):
            self.lock.acquire()
            try:
                bypass_cache = kwargs.pop('bypass_cache', False)
                if not bypass_cache:
                    if self.last_call is not None:
                        if self.ttl is not None:
                            delta = datetime.now() - self.last_call
                            if delta <= self.ttl:
                                return self.cached_val
                self.cached_val = fn(*args, **kwargs)
                self.last_call = datetime.now()
                return self.cached_val
            finally:
                self.lock.release()
        return wrapped_func
