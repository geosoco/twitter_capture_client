import os
from datetime import datetime, time
import unittest
import threading




class RotatingOutFile(object):
	"""
	Rotating File for logging tweets.

	This will keep a file open and write to it until time rolls to the next block, where the old one will be renamed
	"""

	def __init__(self, base_dir = None, collection_name = None, extension = ".json", temporary_extension = ".tmp", minute_interval = 10, filename_timefmt = "%Y%m%d_%H%M"):
		self.extension = extension
		self.temporary_extension = temporary_extension
		self.base_dir = None	# gets officially set in set_collection
		self.collection_name = None		
		self.cur_name = None
		self.minute_interval = minute_interval
		self.filename_timefmt = filename_timefmt
		self.file = None

		self.rlock = threading.RLock()

		self.set_collection(base_dir = base_dir, collection_name = collection_name)


	def make_path(self):
		"""
		makes the file path
		"""

		self.rlock.acquire()

		try:
			path = os.path.join(self.base_dir, self.collection_name)
			if not os.path.exists(path):
				os.makedirs(path)
		finally:
			self.rlock.release()



	def start_file(self, filename):
		"""
		Updates the current filename
		"""

		# acquire the lock
		self.rlock.acquire()

		try:
			# update our filename
			self.cur_name = filename

			# open the file
			self.file = open(self.cur_name, "w")

		finally:
			# release the lock
			self.rlock.release()


	def close_file(self):
		"""
		"""

		# acquire the lock
		self.rlock.acquire()

		try:
			if self.file is not None:
				self.file.close()

			self.file = None


		finally:
			# release the lock
			self.rlock.release()


	def end_file(self):
		"""
		Ends the usage of hte file. 
		This should close the file and rename it to not have the temporary extension
		"""

		# acquire the lock
		self.rlock.acquire()

		try:
			# close the file
			self.close_file()

			# check to see if we need to rename it
			finished_filename = self.cur_name

			if finished_filename is not None:
				if self.temporary_extension and finished_filename.endswith(self.temporary_extension):
					temp_ext_length = len(self.temporary_extension) 
					finished_filename = finished_filename[:-temp_ext_length]

				# rename the file if necessary
				if self.cur_name != finished_filename:
					os.rename(self.cur_name, finished_filename)



		finally:
			# release the lock
			self.rlock.release()



	def set_collection(self, base_dir = None, collection_name = None):
		"""
		Update the collection name.

		Warning, this the potential for a race condition with file/path names
		"""


		changed = False

		# acquire the lock
		self.rlock.acquire()

		try:
			# update if base_dir changed
			if base_dir != self.base_dir:
				self.base_dir = base_dir
				changed = True

			# update if collection changed
			if collection_name != self.collection_name:
				self.collection_name = collection_name
				changed = True

			# if anything changed, update the path
			if changed is True:
				self.update_path()

		finally:
			# release the lock
			self.rlock.release()


	def update_path(self):
		"""

		"""

		# acquire the lock
		self.rlock.acquire()

		try:
			# update the base_filename
			self.base_filename = os.path.join(self.base_dir, self.collection_name, self.collection_name)

			# make our initial path
			self.make_path()
		finally:
			# release the rlock
			self.rlock.release()


	def get_filename(self, datetime_, temp=False ):
		"""
		calculates and returns a new filename based on current time
		it rounds the minute to minute_interval, so 12:12 with a 10-minute interval, will be 12:10
		"""

		name = None

		# acquire the lock
		self.rlock.acquire()

		try:
			# round the da
			base_time = datetime_.time()
			rounded_time = base_time.replace(minute= ((base_time.minute / self.minute_interval) * self.minute_interval))
			rounded_datetime = datetime.combine(datetime_.date(), rounded_time)

			name = self.base_filename + rounded_datetime.strftime(self.filename_timefmt) + self.extension
			if temp == True:
				name += self.temporary_extension
		finally:
			# release the rlock
			self.rlock.release()			

		return name
		


	def write(self, line, datetime_ = None):
		"""
		writes the specified line to the file.

		If datetime_ is None, then it uses datetime.now
		"""

		# acquire the lock
		self.rlock.acquire()

		try:
			# adjust datetime_ to be now, if it's None
			if datetime_ is None:
				datetime_ = datetime.now()

			# test if the filenames have changed
			fn = self.get_filename(datetime_, True)
			if fn != self.cur_name:

				# if the old file is a valid name, "end" it
				if self.cur_name is not None:
					self.end_file()

				self.start_file(fn)

			# write the data
			self.file.write(line + "\n")

		finally:
			# release the rlock
			self.rlock.release()


















#
# unittests
#
#


class MakePathTest(unittest.TestCase):
	def setUp(self):
		import string
		import random

		# use random basenames and collection names to make sure previous failed tests don't cause this to succeed
		n = 8
		self.base_dir = ".unittest-" + ''.join(random.SystemRandom().choice(string.uppercase + string.digits) for _ in xrange(n))
		self.collection = ''.join(random.SystemRandom().choice(string.uppercase + string.digits) for _ in xrange(n))

	def tearDown(self):
		import shutil

		# attempt to remove the entire temporary tree
		shutil.rmtree(self.base_dir)

	def test(self):
		file = RotatingOutFile(
			base_dir = self.base_dir, 
			collection_name = self.collection, 
			extension = ".json", 
			temporary_extension = ".tmp", 
			minute_interval = 10 )

		

class FilenameTest(unittest.TestCase):

	def setUp(self):
		self.base_dir = ".unittests"
		self.collection = "test"
		self.ext = ".json"
		self.tmp_ext = ".tmp"
		self.dt = datetime(1997,10,30,12,56,0,0)
		self.dt_string = "19971030_1250"
		self.filename = os.path.join(
			self.base_dir, 
			self.collection, 
			self.collection) 

		self.file = RotatingOutFile(
			base_dir = self.base_dir, 
			collection_name = self.collection, 
			extension = ".json", 
			temporary_extension = ".tmp", 
			minute_interval = 10 )

	def tearDown(self):
		import shutil

		# attempt to remove the entire temporary tree
		shutil.rmtree(self.base_dir)

	def test_makepath(self):
		unittest_path = os.path.join(
			self.base_dir,
			self.collection)
		self.assertTrue(os.path.exists(unittest_path))




	def test_path_concat(self):
		"""
		Tests teh basic path concat methods for a time
		"""
		filename = self.filename + self.dt_string + self.ext
		result = self.file.get_filename(self.dt)
		self.assertEqual(result, filename, "paths are not equal: '%s', '%s'"%(result, filename))

	def test_path_concat_temp(self):
		"""
		tests that the temporary extension is concatenated
		"""
		filename = self.filename + self.dt_string + self.ext + self.tmp_ext
		result = self.file.get_filename(self.dt, True)
		self.assertEqual(result,filename, "paths are not equal: '%s', '%s'"%(result, filename))


	def test_write(self):
		"""
		"""
		import string
		import random

		filename = self.file.get_filename(self.dt, True)

		# randomize a string for content
		test_content = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(32))

		# write content
		self.file.write(test_content, self.dt)
		self.file.close_file()

		# verify it was created
		self.assertTrue(os.path.exists(filename), "Temp file wasn't created (%s)"%(filename))

		# attempt to verify the contents 
		with open(filename,"r") as f:
			content = f.read()
			self.assertEqual(test_content + "\n", content)





if __name__ == '__main__':
    unittest.main()

