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

import re


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

        self.geo_regex = re.compile(
            r"geo:\s(-?[\d.]+)\s+(-?[\d.]+)\s+(-?[\d.]+)\s+(-?[\d.]+)",
            re.I)

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

        if self.keywords is not None and len(self.keywords) > 0:
            track_args = self.split_keyword_args()
            self.log.debug("logging keywords: %s", repr(track_args))
            self.stream.filter(**track_args)
        else:
            self.stream.sample()

    def parse_geo_rect(self, geo_string):
        m = self.geo_regex.match(geo_string)
        if m is not None:
            arr = [float(a) for a in m.groups()]
            geolong = sorted([arr[0], arr[2]])
            geolat = sorted([arr[1], arr[3]])

            return [
                geolong[0],
                geolat[0],
                geolong[1],
                geolat[1]
            ]

        return None

    def split_keyword_args(self):
        """parse out geo terms"""
        keywords = []
        locations = []
        ret = {}

        for k in self.keywords:
            parsed = self.parse_geo_rect(k)
            if parsed is None:
                if k is not None and len(k.strip()) > 0:
                    #self.log.info("keyword", repr(k))
                    keywords.append(k)
            else:
                locations.extend(parsed)

        if len(keywords) > 0:
            ret["track"] = keywords

        if len(locations) > 0:
            ret["locations"] = locations

        return ret


    def isRunning(self):
        """start running collection"""
        pass
