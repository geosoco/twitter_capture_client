from time import time
from twitter_monitor import JsonStreamListener
import os
from datetime import datetime
import logging
from rotating_out_file import RotatingOutFile
import simplejson as json

log = logging.getLogger(__name__)


class BaseListener(JsonStreamListener):
	"""
	Base listener that implements some counting mechanisms
	"""
	def __init__(self, api=None):
		super(BaseListener, self).__init__(api)
		self.terminate = False
		self.received = 0
		self.total = 0
		self.since = time()
		self.rate = 0
		self.connected = False
		log.debug("BaseListener constructed")

	def on_connect(self):
		super(BaseListener, self).on_connect()
		self.connected = True
		return True

	def on_disconnect(self):
		super(BaseListener, self).on_disconnect()
		self.disconnected = True
		return True

	def on_status(self, status):
		self.received += 1
		self.total += 1
		return not self.terminate

	def set_terminate(self):
		"""Notify the tweepy stream that it should quit"""
		self.terminate = True

	def print_status(self):
		"""Print out the current tweet rate and reset the counter"""
		tweets = self.received
		now = time()
		diff = now - self.since
		self.since = now
		self.received = 0
		self.rate = (tweets/diff) if diff > 0 else 0
		log.info("Receiving tweets at %s tps", self.rate)



class PrintingListener(BaseListener):
	"""
	Printing Listener
	"""

	def __init__(self, api=None):
		super(PrintingListener, self).__init__(api)


	def on_status(self, status):
		retval = super(PrintingListener, self).on_status(self, status)
		print json.dumps(status)

		return retval



class FileListener(BaseListener):
	"""
	File Listener
	"""


	def __init__(self, base_dir, collection_name, api=None):
		super(FileListener, self).__init__(api)
		self.base_dir = base_dir
		self.collection_name = collection_name
		self.base_filename = os.join(self.base_dir, self.collection_name, self.collection_name)

	def on_connect(self):
		super(FileListener, self).on_connect()
		self.file = open(self.base_filename, "a")
		self.connected = True
		return True

	def on_disconnect(self):
		super(FileListener, self).on_disconnect()
		self.disconnected = True
		return True

	def on_status(self, status):
		retval = super(FileListener, self).on_status(status)
		if self.file is not None:
			self.file.write(status + "\n")
		return retval



class RotatingFileListener(BaseListener):
	"""
	Rotating File Listener
	"""


	def __init__(self, base_dir = None, collection_name = None, extension = ".json", temporary_extension = ".tmp", minute_interval = 10, filename_timefmt = "%Y%m%d_%H%M", api = None):
		super(RotatingFileListener, self).__init__(api)
		self.base_dir = base_dir
		self.collection_name = collection_name
		self.file = RotatingOutFile(
			base_dir = self.base_dir,
			collection_name = self.collection_name
			)
		#self.base_filename = os.path.join(self.base_dir, self.collection_name, self.collection_name)


	def on_connect(self):
		retval = super(RotatingFileListener, self).on_connect()
		self.connected = True
		return retval 

	def on_disconnect(self):
		retval = super(RotatingFileListener, self).on_disconnect()
		self.disconnected = True
		log.info("RotatingFileListener - disconnect")
		self.file.end_file()
		return retval 

	def on_status(self, status):
		self.file.write(json.dumps(status))
		return super(RotatingFileListener, self).on_status(status)



