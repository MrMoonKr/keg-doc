#!/usr/bin/env python
import csv
import logging
import os
import requests
from configparser import RawConfigParser
from io import StringIO
from hashlib import md5


logging.basicConfig()
logger = logging.getLogger("keg")
logger.setLevel(logging.DEBUG)
log = logger.info
debug = logger.debug


class ServerConfigurationError(Exception):
	pass


class NGDPCache:
	HOME = os.path.expanduser("~")
	XDG_CACHE_HOME = os.environ.get("XDG_CACHE_HOME", os.path.join(HOME, ".cache"))

	def __init__(self, domain):
		self.domain = domain
		self.basedir = os.path.join(self.XDG_CACHE_HOME, domain)
		if not os.path.exists(self.basedir):
			os.makedirs(self.basedir)

	def __contains__(self, path):
		path = os.path.join(self.basedir, path.strip("/"))
		return os.path.exists(path)

	def get(self, path):
		path = os.path.join(self.basedir, path.strip("/"))
		with open(path, "rb") as f:
			return f.read()

	def write(self, key, data, hash):
		debug("write_to_cache(key=%r, data=%r, hash=%r", key, len(data), hash)
		dirname = os.path.join(self.basedir, key)
		if not os.path.exists(dirname):
			# debug("mkdir %r", dirname)
			os.makedirs(dirname)
		fname = os.path.join(dirname, hash)
		with open(fname, "wb") as f:
			f.write(data)
		log("Written %i bytes to %r" % (len(data), fname))


def split_key_and_hash(path):
	key, hash = os.path.split(path)
	key = key.strip("/")
	return key, hash


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
			self.cache.write("cdns", r.content, md5(r.content).hexdigest())
			reader = csv.reader(StringIO(r.text), delimiter="|")
			self._obj_cache[path] = self._parse_csv(reader)
		return self._obj_cache[path]

	def get_config(self, hash):
		data = self.cdn_get("/config/{0}/{1}/{2}".format(hash[0:2], hash[2:4], hash))
		config = RawConfigParser()
		config.readfp(StringIO("[DEFAULT]" + data.decode("utf-8")))
		return dict(config["DEFAULT"])

	def cdn_get(self, path):
		if path not in self.cache:
			r = requests.get(self.cdn + path)
			key, hash = split_key_and_hash(path)
			self.cache.write(key, r.content, hash)
		return self.cache.get(path)

	def get(self, path):
		return requests.get(self.host + path)


def main():
	url = "http://{region}.patch.battle.net:1119/hsb"
	region = "eu"
	conn = NGDPConnection(url, region)

	for v in conn.versions:
		print(v)


if __name__ == "__main__":
	main()
