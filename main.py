#!/usr/bin/env python
import csv
import logging
import os
import requests
from collections import OrderedDict
from io import StringIO
from hashlib import md5


logging.basicConfig()
logger = logging.getLogger("keg")
logger.setLevel(logging.DEBUG)
log = logger.info
debug = logger.debug


def split_hash(hash):
	return hash[0:2], hash[2:4], hash


class ServerError(Exception):
	pass


class ServerConfigurationError(ServerError):
	pass


class FlatINI(OrderedDict):
	"""
	An OrderedDict with optional multiple values per key.
	Can read from a "flat ini" file.
	"""
	def readfp(self, f):
		for line in f.readlines():
			line = line.strip()
			if not line or line.startswith("#"):
				continue
			key, sep, value = line.partition("=")
			key = key.strip()
			value = value.strip()
			self[key] = value.strip()

	def __setitem__(self, key, value):
		if key in self:
			if not isinstance(self[key], list):
				super().__setitem__(key, [self[key]])
			self[key].append(value)
		else:
			super().__setitem__(key, value)

	def items(self):
		for k, v in super().items():
			if isinstance(v, list):
				for item in v:
					yield k, item
			else:
				yield k, v

	def keys(self):
		for k, v in super().items():
			if isinstance(v, list):
				for item in v:
					yield k
			else:
				yield k

	def values(self):
		for k, v in super().values():
			if isinstance(v, list):
				for item in v:
					yield item
			else:
				yield v

	def __str__(self):
		return "\n".join("{} = {}".format(k, v) for k, v in self.items())


class NGDPCache:
	HOME = os.path.expanduser("~")
	XDG_CACHE_HOME = os.environ.get("XDG_CACHE_HOME", os.path.join(HOME, ".cache"))

	def __init__(self, domain, basedir=XDG_CACHE_HOME):
		self.domain = domain
		self.basedir = os.path.join(basedir, domain)

	def contains(self, key, name):
		path = os.path.join(self.basedir, key, name)
		return os.path.exists(path)

	def get(self, key, name):
		path = os.path.join(self.basedir, key, name)
		with open(path, "rb") as f:
			return f.read()

	def write(self, key, name, data, hash):
		debug("write_to_cache(key=%r, name=%r, data=%r, hash=%r", key, name, len(data), hash)
		dirname = os.path.join(self.basedir, key)
		if not os.path.exists(dirname):
			# debug("mkdir %r", dirname)
			os.makedirs(dirname)
		fname = os.path.join(dirname, name)
		with open(fname, "wb") as f:
			f.write(data)
		log("Written %i bytes to %r" % (len(data), fname))


class NGDPConnection:
	def __init__(self, url, region="eu"):
		self.host = url.format(region=region)
		self.region = region
		self.cache = NGDPCache("info.hearthsim.keg")
		self._obj_cache = {}
		self._cdn_host = None
		self._build_config = None
		self.verify = False

	@property
	def cdns(self):
		return self._get_cached_csv("/cdns")

	@property
	def cdn(self):
		if not self._cdn_host:
			cdns = self.cdns
			if not cdns:
				raise ServerConfigurationError("No CDN available")
			for cdn in cdns:
				if cdn["Name"] == self.region:
					break
			else:
				cdn = cdns[0]
			cdn_host = cdn["Hosts"].split(" ")[0]
			self._cdn_host = "http://{cdn}/{path}/".format(cdn=cdn_host, path=cdn["Path"])
		return self._cdn_host

	@property
	def versions(self):
		for row in self._get_cached_csv("/versions"):
			if row["Region"] == self.region:
				row["BuildConfig"] = self.get_config(row["BuildConfig"])
				row["CDNConfig"] = self.get_config(row["CDNConfig"])
				yield row

	def _parse_csv(self, rows):
		rows = list(rows)
		columns = rows[0]
		column_names = [c.split("!")[0] for c in columns]
		ret = []
		for row in rows[1:]:
			ret.append({k: v for k, v in zip(column_names, row)})
		return ret

	def _get_cached_csv(self, path):
		if path not in self._obj_cache:
			r = self.get(path)
			hash = md5(r.content).hexdigest()
			self.cache.write("cdns", hash, r.content, hash)
			reader = csv.reader(StringIO(r.text), delimiter="|")
			self._obj_cache[path] = self._parse_csv(reader)
		return self._obj_cache[path]

	def get_or_cache(self, key, hash, name=None):
		if name is None:
			name = hash

		if not self.cache.contains(key, name):
			data = self.cdn_get("{0}/{1}/{2}/{3}".format(key, *split_hash(name)))
			self.cache.write(key, name, data, hash)
		else:
			data = self.cache.get(key, name)

		return data

	def get_config(self, hash):
		data = self.get_or_cache("config", hash)
		config = FlatINI()
		config.readfp(StringIO(data.decode("utf-8")))
		return config

	def get_data(self, hash):
		index = self.get_or_cache("data", hash, name=hash + ".index")
		data = self.get_or_cache("data", hash)
		return index, data

	def get_patch(self, hash):
		data = self.get_or_cache("patch", hash)
		return data

	def cdn_get(self, path):
		url = self.cdn + path
		debug("GET %s", url)
		r = requests.get(url)
		if r.status_code != 200:
			raise ServerError("Got HTTP %r when querying %r" % (r.status_code, url))
		return r.content

	def get(self, path):
		return requests.get(self.host + path)


def main():
	url = "http://{region}.patch.battle.net:1119/hsb"
	region = "eu"
	conn = NGDPConnection(url, region)
	conn.cache.basedir = "./cache"

	for v in conn.versions:
		build = v["BuildId"]
		build_name = v["VersionsName"]
		print("Found build %s (%r)" % (build_name, build))
		for archive in v["CDNConfig"]["archives"].split(" "):
			conn.get_data(archive)

		patch_ekey = v["BuildConfig"]["patch"]
		conn.get_patch(patch_ekey)

		patch_config = conn.get_config(v["BuildConfig"]["patch-config"])
		assert patch_config["patch"] == patch_ekey


if __name__ == "__main__":
	main()
