#!/usr/bin/env python
"""Twitter capture client code."""

import logging
from time import sleep
from datetime import datetime


from listeners.file import RotatingFileListener
from configfile import ConfigFile
from server_messenger import ServerMessenger
from streamer import Streamer



log = logging.getLogger(__name__)


class TwitterClient(object):

    """Twitter Client"""

    def __init__(self, server_messenger):
        """initialize the twitter client"""
        self.ping_interval = 10
        self.update_interval = 10
        self.twitter_auth = None
        self.output_config = None
        self.source_addr = None



    def config(self, config):
