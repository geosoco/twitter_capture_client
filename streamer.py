#!/usr/bin/env python
"""Streamer class."""
import tweepy
import logging
from time import sleep
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.poolmanager import PoolManager

log = logging.getLogger(__name__)



class SourceAddressAdapter(HTTPAdapter):

    """Adapter for requests to fix the source ip address.

    Taken from (https://github.com/kennethreitz/requests/issues/2008)
    """

    def __init__(self, source_address, **kwargs):
        """Store the source address."""
        self.source_address = source_address

        super(SourceAddressAdapter, self).__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False):
        """Initialize the poolmanager."""
        self.poolmanager = PoolManager(num_pools=connections,
                                       maxsize=maxsize,
                                       block=block,
                                       source_address=self.source_address)



class SourceAddrStreamer(tweepy.Stream):

    """Tweepy Stream wrapper for handling using source address adapter."""

    def __init__(self, auth, listener, source_addr=None, **kwargs):
        """store source address."""
        self.source_addr = source_addr
        self.disconnected = False
        return super(SourceAddrStreamer, self).__init__(
            auth,
            listener,
            **kwargs
        )

    def new_session(self):
        """create new session with the source address."""
        super(SourceAddrStreamer, self).new_session()
        if self.source_addr is not None:
            self.session.mount(
                "http://",
                SourceAddressAdapter((self.source_addr, 0))
            )
            self.session.mount(
                "https://",
                SourceAddressAdapter((self.source_addr, 0))
            )



    def on_closed(self, resp):
        """make sure to call disconnected."""
        super(SourceAddrStreamer, self).on_closed(resp)


        self.disconnected = True
        if self.running is True:
            self.disconnect()




class Streamer(object):

    """Streamer class."""

    def __init__(
            self,
            listener,
            api_key,
            api_secret,
            access_token,
            access_token_secret,
            source_addr=None
    ):
        """initialize the streamer object."""

        self.api_key = api_key
        self.api_secret = api_secret
        self.access_token = access_token
        self.access_token_secret = access_token_secret

        self.listener = listener
        self.auth = None

        self.source_addr = source_addr

        self.stream = None
        self.track_list = []

        self.doAuth()


    def doAuth(self):
        """perform authorization."""

        log.debug("doAuth")
        self.auth = tweepy.OAuthHandler(self.api_key, self.api_secret)
        self.auth.set_access_token(self.access_token, self.access_token_secret)


    def start(self):
        """start the streaming capture."""

        if self.auth is None:
            self.doAuth()


        if self.stream is None:
            self.stream = SourceAddrStreamer(
                self.auth,
                self.listener,
                source_addr=self.source_addr,
                stall_warnings=True,
                timeout=90,
                retry_count=5)

            self.stream.filter(track=self.track_list)




    def stop(self):
        """stop the streaming capture."""

        if self.stream is not None:

            log.info("stopping...")
            self.stream.disconnect()

            while self.stream.running:
                log.info("waiting for stream to stop...")
                sleep(1)

            self.stream = None


    def isRunning(self):
        """return true if stream is running."""

        if self.stream is not None and self.stream.running:
            return self.stream.running

        return False
