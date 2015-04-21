import os
import simplejson as json


class ConfigFile(object):
	"""
	Simple JSON config file class
	"""

	def __init__(self, filename):
		"""
		initialize data and call to load the data
		"""
		self.filename = filename
		self.config_data = {}

		self.loadConfig(self.filename)


	def loadConfig(self, filename):
		"""
		load the config file
		"""
		with open(filename) as f:
			self.config_data = json.load(f)


	def getValue(self, path, defaultValue = None):
		"""
		gets a value from the config data.
		can also take a path using the period as a separator. eg "first.second.third"
		"""

		if not path:
			return None

		parts = path.split(".")
		num_parts = len(parts)
		cur_dict = self.config_data

		try:
			for i in range(0, num_parts-1):
				part = parts[i]
				print part
				cur_dict = cur_dict[part]

			return cur_dict[ parts[num_parts-1] ]
		except KeyError, e:
			return defaultValue


	def __len__(self):
		"""
		returns the length from len()
		"""
		return len(self.config_data)

	def __getitem__(self, key):
		"""
		returns the item like a dictionary
		"""
		return self.config_data[key]

	def __setitem__(self, key, value):
		"""
		sets the item like a dictionary
		"""
		self.config_data[key] = value

	def __delitem__(self, key):
		"""
		deletes a config item by its key
		"""
		del self.config_data[key]

	def __iter__(self):
		"""
		returns an key iterator for the config data
		"""
		return self.config_data.iterkeys()

	def __contains__(self, key):
		"""
		returns True if the key is in the config file
		"""
		return key in self.config_data


if __name__ == "__main__":

	cf = ConfigFile("config.json")

	print cf.getValue("twitter_auth.api_key")

	print cf.getValue("twitter_auth")

	for k in cf:
		print k

