#!/usr/bin/env python
"""Base classes for clients."""



class BaseClient(object):

    """Base class for the client"""

    def __init__(
            self,
            config):
        """construct client."""

        self.listener = None

    def configure(self, config):
        """configure the client"""
        pass


    def initialize(self):
        """initialize the client"""
        pass


    def shutdown(self):
        """shutdown the client"""
        pass


    def run(self):
        """run the core collection loop"""
        pass
    

    def isRunning(self):
        """start running collection"""
        pass

