import requests
import logging
import simplejson as json
from decimal import *

log = logging.getLogger(__name__)




class ServerMessenger(object):
	"""
	Wrapper class for messaging with the server
	"""


	def __init__(self, base_url, token):
		self.base_url = base_url
		self.token = token
		self.active_job_id = -1
		self.headers = {'Authorization': 'Token %s'%(self.token) }

		# log details
		log.debug("initializing(base_url=%s, token=%s, client_id=%d"%(self.base_url, self.token, self.active_job_id))
		log.debug("headers (%s)"%(self.headers))



	def doGet(self, endpoint, args = None):
		"""
		Wrapper around requests get function
		"""

		url = self.base_url + endpoint
		log.debug("doGet - url = %s", url)

		try:
			# send params if available, otherwise, just the request
			if args is not None:
				log.debug("params: %s", repr(args))
				ret = requests.get( url, params=args, headers = self.headers )
			else:
				ret = requests.get( url, headers = self.headers)

			# log some debug output
			log.debug("request returned: %d %s", ret.status_code, ret.text)
			if ret is not None and ret.status_code != requests.codes.ok:
				log.error("request error: %d %s", ret.status_code, ret.text)

			# return our value
			return ret

		except Exception, e:
			log.exception("exception occurred: %s", e.message)
			return None


	def doSimpleJSONGet(self, endpoint, args = None):
		"""
		Simple wrapper function get to get json data
		"""
		resp = self.doGet(endpoint)

		if resp is not None:
			if resp.status_code == requests.codes.ok:
				log.debug('server returned: %s', resp.json())
				return resp.json()
			else:
				log.error("server returned error code: %d", resp.status_code)
		else:
			log.error("doGet returned None")

		return None


	def doPut(self, endpoint, data = None):
		"""
		Wrapper around requests put function
		"""

		url = self.base_url + endpoint
		log.debug("doPut - url = %s", url)

		try:
			if data is not None:
				log.debug("put data: %s", repr(data))
				ret = requests.put( url, data=data, headers = self.headers)
			else:
				ret = requests.put( url, headers=self.headers)

			# log some debug output
			log.debug("put request returned: %d %s", ret.status_code, ret.text)
			if ret is not None and ret.status_code < requests.codes.ok or ret.status_code > requests.codes.accepted:
				log.error("put request error: %d %s", ret.status_code, ret.text)

			# return our value
			return ret

		except Exception, e:
			log.exception("exception in doPut")
			return None

	def doPost(self, endpoint, data = None):
		"""
		Wrapper around requests post function
		"""

		url = self.base_url + endpoint
		log.debug("doPost - url = %s", url)

		try:
			if data is not None:
				log.debug("post data: %s", repr(data))
				ret = requests.post( url, data=data, headers = self.headers)
			else:
				ret = requests.post( url, headers=self.headers)

			# log some debug output
			log.debug("post request returned: %d %s", ret.status_code, ret.text)
			if ret is not None and ret.status_code < requests.codes.ok or ret.status_code > requests.codes.accepted:
				log.error("post request error: %d %s", ret.status_code, ret.text)

			# return our value
			return ret

		except Exception, e:
			log.exception("exception in doPost")
			return None


	def doPatch(self, endpoint, data = None):
		"""
		Wrapper around requests patch function
		"""

		url = self.base_url + endpoint
		log.debug("doPatch - url = %s", url)

		try:
			if data is not None:
				log.debug("patch data: %s", repr(data))
				ret = requests.patch( url, data=data, headers = self.headers)
			else:
				ret = requests.patch( url, headers=self.headers)

			# log some debug output
			log.debug("patch request returned: %d %s", ret.status_code, ret.text)
			if ret is not None and ret.status_code < requests.codes.ok or ret.status_code > requests.codes.accepted:
				log.error("patch request error: %d %s", ret.status_code, ret.text)

			# return our value
			return ret

		except Exception, e:
			log.exception("exception in doPatch")
			return None




	def getStatus(self):
		"""
		gets current job status
		"""

		endpoint = "jobs/%d/"%(self.active_job_id)
		return self.doSimpleJSONGet(endpoint)


	def putStatus(self, status_obj):
		"""
		updates the status
		"""

		endpoint = "jobs/%d/"%(self.active_job_id)
		resp = self.doPut(endpoint=endpoint, data=status_obj)

		if resp is not None:
			if resp.status_code == requests.codes.created:
				log.debug('server returned: %s', resp.json())	
				return resp.json()
			else:
				log.error("server return error code on put: %d", resp.status_code)
		else:
			log.error("doPut returned None")

		return None



	def updateStatus(self, status):
		"""
		"""

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
			log.error("can't update status because getStatus returned None")




	def putUpdate(self, num_tweets, total_tweets, rate ):
		"""
		Puts an update object on the server
		"""

		update_msg = {
			"count": num_tweets,
			"total_count": total_tweets,
			"rate": Decimal(rate).quantize(Decimal('0.001'), rounding=ROUND_DOWN),
			"job": self.active_job_id
		}

		endpoint = "update/"
		resp = self.doPost(endpoint=endpoint, data=update_msg)


	def pingServer(self, total_tweets, rate):
		update_msg = {
			"total_count": total_tweets,
			"rate": Decimal(rate).quantize(Decimal('0.001'), rounding=ROUND_DOWN)
		}

		endpoint = "jobs/%d/"%(self.active_job_id)
		resp = self.doPatch(endpoint=endpoint, data=update_msg)


	def putLogMessage(self, message):
		pass


	def getActiveJobs(self):
		"""
		requests the current active job for the client
		"""
		endpoint = "activejobs/"
		return self.doSimpleJSONGet(endpoint)


