#!/usr/bin/env python
"""This is a simple json config file module."""

import simplejson as json
import copy


class ConfigFile(object):

    """Simple JSON config file class."""

    def __init__(self, filename=None, config_data=None):
        """initialize data and call to load the data."""
        self.filename = filename
        self.config_data = {} if config_data is None else config_data

        if filename is not None:
            self.loadConfig(self.filename)

        self.extend(self.config_data)


    def loadConfig(self, filename):
        """load the config file."""
        with open(filename) as f:
            self.config_data = json.load(f)

    def dict_merge(self, target, base):
        target_copy = copy.deepcopy(target)
        for k, v in base.iteritems():
            target_exists = k in target

            # if it doesn't exist clone it
            if not target_exists:
                target_copy[k] = copy.deepcopy(v)
            elif isinstance(v, dict):
                # merging is necessary
                target_val = target.get(k, None)
                if target_val is not None:
                    if isinstance(target_val, dict):
                        merged = self.dict_merge(target_val, base[k])
                        target_copy[k] = merged
                    else:
                        raise Exception(
                            "source value is not a dict but target is.")

        return target_copy


    def getNestedValue(self, path_array, default=None):
        """ gets value from a nested value """
        # step through each path and try to process it
        for cur_path in path_array:
            parts = cur_path.split(".")
            num_parts = len(parts)
            cur_dict = self.config_data

            # step through each part of the path
            try:
                for i in range(0, num_parts - 1):
                    part = parts[i]
                    cur_dict = cur_dict[part]

                return cur_dict[parts[num_parts - 1]]
            except KeyError:
                pass

        return default



    def getValue(self, path, default=None, alternate_paths=None):
        """
        get a value from the config data.

        `path` is a single string representing a list of keys
        as a period-separated path to the value. eg. "first.second.third"
        will grab the value from:

        {
            "first": {
                "second": {
                    "third": 42
                }
            }
        }

        `alternate_paths` is a list of possible paths to also look

        """

        if not path:
            return None

        # create a full list of paths to check
        path_list = [path]
        if alternate_paths is not None:
            path_list.extend(alternate_paths)

        return self.getNestedValue(path_list, default=default)


    def extend(self, branch=None):
        """ extend  dictionary """

        # if not iterating, start with cloned root data
        if branch is None:
            branch = self.config_data

        config_copy = copy.copy(branch)
        for k, v in config_copy.iteritems():
            if isinstance(v, dict):
                self.extend(v)

                extension_path = v.get("_extend", None)
                if extension_path is not None:
                    base_dict = self.getValue(extension_path)
                    branch[k] = self.dict_merge(v, base_dict)





    def __len__(self):
        """return the length from len()."""
        return len(self.config_data)


    def __getitem__(self, key):
        """return the item like a dictionary."""
        return self.config_data[key]


    def __setitem__(self, key, value):
        """set the item like a dictionary."""
        self.config_data[key] = value


    def __delitem__(self, key):
        """delete a config item by its key."""
        del self.config_data[key]


    def __iter__(self):
        """return an key iterator for the config data."""
        return self.config_data.iterkeys()


    def __contains__(self, key):
        """return True if the key is in the config file."""
        return key in self.config_data


if __name__ == "__main__":

    cf = ConfigFile("config.json")

    print cf.getValue("twitter_auth.api_key")

    print cf.getValue("twitter_auth")

    for k in cf:
        print k
