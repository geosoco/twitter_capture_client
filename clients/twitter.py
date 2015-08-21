#!/usr/bin/env python
"""Twitter capture client code."""

import logging
from time import sleep
from datetime import datetime
import tweepy

from listeners.file import RotatingFileListener
from configfile import ConfigFile
from server_messenger import ServerMessenger
from streamer import SourceAddrStreamer



class TwitterClient(object):

    """Twitter Client"""

    def __init__(self, listener, keywords=None):
        """initialize the twitter client"""
        self.update_interval = 10
        self.twitter_auth = None
        self.source_addr = None

        self.listener = listener
        self.stream = None
        self.keywords = keywords
        self.auth = None

        self.log = logging.getLogger(self.__class__.__name__)


    def configure(self, config):
        """configure the client"""
        self.twitter_auth = config.getValue("twitter_auth", None)
        self.source_addr = config.getValue("source_addr", None)
        self.update_interval = config.getValue("server.update_interval", 60.0)


    def initialize(self):
        """initialize the client"""

        api_key = self.twitter_auth["api_key"]
        api_secret = self.twitter_auth["api_secret"]
        access_token = self.twitter_auth["access_token"]
        access_token_secret = self.twitter_auth["access_token_secret"]


        # auth
        self.log.debug("doAuth")
        self.auth = tweepy.OAuthHandler(api_key, api_secret)
        self.auth.set_access_token(access_token, access_token_secret)

        # create streamer
        self.stream = SourceAddrStreamer(
            self.auth,
            self.listener,
            source_addr=self.source_addr,
            stall_warnings=True,
            timeout=300,
            retry_count=10)


    def shutdown(self):
        """shutdown the client"""
        pass


    def run(self):
        """run the core collection loop"""
        if self.keywords is not None:
            self.stream.filter(track=self.keywords)
        else:
            self.stream.sample()


    def isRunning(self):
        """start running collection"""
        pass
