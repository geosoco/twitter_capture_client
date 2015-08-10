#!/usr/bin/env python
"""server messenger class."""

import requests
import logging
from datetime import timedelta
from decimal import Decimal, ROUND_DOWN
from utils.cache_decorators import cached_function_ttl

log = logging.getLogger(__name__)


#
#
#
#
#
class CaptureStatus(object):

    """ Server status wrapper"""

    STATUS_UNKNOWN = 0
    STATUS_CREATED = 1
    STATUS_STARTING = 2
    STATUS_STARTED = 3
    STATUS_STOPPING = 4
    STATUS_STOPPED = 5
    STATUS_UNRESPONSIVE = 6
    STATUS_DEAD = 7

    status_names = {
        0: "STATUS_UNKNOWN",
        1: "STATUS_CREATED",
        2: "STATUS_STARTING",
        3: "STATUS_STARTED",
        4: "STATUS_STOPPING",
        5: "STATUS_STOPPED",
        6: "STATUS_UNRESPONSIVE",
        7: "STATUS_DEAD"
    }

    @staticmethod
    def isRunning(status):
        """return true if status is running"""
        return (status == CaptureStatus.STATUS_STARTED or
                status == CaptureStatus.STATUS_STARTING)

    @staticmethod
    def isStopped(status):
        """return true if status is stopped"""
        return (not (
            status == CaptureStatus.STATUS_STARTED or
            status == CaptureStatus.STATUS_STARTING))

    def __init__(self, status):
        self.status = status

    @property
    def running(self):
        """return true if status is starting or started"""
        return CaptureStatus.isRunning(self.status)

    @property
    def stopped(self):
        """ return true if status is not starting or started"""
        return CaptureStatus.isStopped(self.status)

    @property
    def value(self):
        return self.status

    def __str__(self):
        if self.status in self.status_names:
            return "%s (%d)" % (self.status_names[self.status], self.status)
        return "<<INVALID STATUS (%d)>>" % (self.status)

    def __repr__(self):
        return str(self)

    def __unicode__(self):
        return unicode(str(self))


#
#
#
#
#
class ServerMessenger(object):

    """Wrapper class for messaging with the server."""

    def __init__(self, base_url, token):
        """initialize the server messenger."""
        self.base_url = base_url
        self.token = token
        self.active_job_id = -1
        self.headers = {
            'Authorization': 'Token %s' % (self.token)
        }

        # log details
        log.debug("initializing(base_url=%s, token=%s, client_id=%d" % (
            self.base_url,
            self.token,
            self.active_job_id)
        )
        log.debug("headers (%s)" % (self.headers))


    def request(self, method, endpoint, params=None, data=None):
        """request wrapper"""
        url = self.base_url + endpoint
        log.debug(
            "request(%s, '%s', params=%s, data=%s)",
            method,
            url,
            repr(params),
            repr(data))

        ret = None

        try:
            ret = requests.request(
                method,
                url,
                params=params,
                data=data,
                headers=self.headers)

            # log some debug output
            log.debug("request returned: %d %s", ret.status_code, ret.text)
            if (ret is not None and ret.status_code < requests.codes.ok or
                    ret.status_code > requests.codes.accepted):
                log.error("request error: %d %s", ret.status_code, ret.text)
                return None

            # return our value
            return ret
        except Exception, e:
            log.exception("exception occurred: %s", e.message)

    def get(self, endpoint, params=None, data=None):
        """ perform http get """
        return self.request("GET", endpoint, params=params, data=data)


    def doSimpleJSONGet(self, endpoint, args=None):
        """get simple json from endpoint."""
        resp = self.get(endpoint, params=args)

        if resp is not None:
            log.debug('server returned: %s', resp.json())
            return resp.json()
        else:
            log.error("get returned None")

        return None


    def doPut(self, endpoint, data=None):
        """put request to server."""
        return self.request("PUT", endpoint, data=data)


    def doPost(self, endpoint, data=None):
        """post to server."""
        return self.request("POST", endpoint, data=data)


    def doPatch(self, endpoint, data=None):
        """send patch message to server."""
        return self.request("PATCH", endpoint, data=data)


    # this isn't the best way to do this
    # this is hardcoded :\
    @cached_function_ttl(timedelta(seconds=3))
    def getStatus(self):
        """get current job status."""

        endpoint = "jobs/%d/" % (self.active_job_id)
        return self.doSimpleJSONGet(endpoint)


    def putStatus(self, status_obj):
        """update the status."""

        endpoint = "jobs/%d/" % (self.active_job_id)
        resp = self.put(endpoint=endpoint, data=status_obj)

        if resp is not None:
            if resp.status_code == requests.codes.created:
                log.debug('server returned: %s', resp.json())
                return resp.json()
            else:
                log.error(
                    "server return error code on put: %d",
                    resp.status_code)
        else:
            log.error("doPut returned None")

        return None



    def updateStatus(self, status):
        """update the status."""

        status_msg = self.getStatus()
        if status_msg is not None:
            if "status" in status_msg:
                old_status = status_msg["status"]

                if old_status != status:
                    status_msg["status"] = status
                    self.putStatus(status_msg)
                else:
                    log.warn("attempt to update status and its already set")
            else:
                log.error("status message is invalid")
        else:
            log.error("can't update status because getStatus returned None")




    def putUpdate(self, num_tweets, total_tweets, rate):
        """Put an update object on the server."""

        update_msg = {
            "count": num_tweets,
            "total_count": total_tweets,
            "rate": Decimal(rate).quantize(
                Decimal('0.001'),
                rounding=ROUND_DOWN),
            "job": self.active_job_id
        }

        endpoint = "update/"
        resp = self.doPost(endpoint=endpoint, data=update_msg)
        return resp


    def pingServer(self, total_tweets, rate):
        """ping the server."""
        decimal_rate = Decimal(rate).quantize(
            Decimal('0.001'),
            rounding=ROUND_DOWN)
        update_msg = {
            "total_count": total_tweets,
            "rate": decimal_rate
        }

        endpoint = "jobs/%d/" % (self.active_job_id)
        resp = self.doPatch(endpoint=endpoint, data=update_msg)
        return resp


    def putLogMessage(self, message):
        """put log message on server."""
        pass


    # this cached function decorator is not the right approach
    # this should be configurable
    @cached_function_ttl(timedelta(seconds=3))
    def getActiveJobs(self):
        """request the current active job for the client."""
        endpoint = "activejobs/"
        return self.doSimpleJSONGet(endpoint)
