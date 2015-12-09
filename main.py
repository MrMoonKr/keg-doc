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

	def __init__(self, domain, basedir=XDG_CACHE_HOME):
		self.domain = domain
		self.basedir = os.path.join(basedir, domain)

	def contains(self, key, hash):
		path = os.path.join(self.basedir, key, hash)
		return os.path.exists(path)

	def get(self, key, hash):
		path = os.path.join(self.basedir, key, hash)
		with open(path, "rb") as f:
			return f.read()

	def write(self, key, hash, data):
		debug("write_to_cache(key=%r, data=%r, hash=%r", key, len(data), hash)
		dirname = os.path.join(self.basedir, key)
		if not os.path.exists(dirname):
			# debug("mkdir %r", dirname)
			os.makedirs(dirname)
		fname = os.path.join(dirname, hash)
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
			self.cache.write("cdns", md5(r.content).hexdigest(), r.content)
			reader = csv.reader(StringIO(r.text), delimiter="|")
			self._obj_cache[path] = self._parse_csv(reader)
		return self._obj_cache[path]

	def get_config(self, hash):
		key = "config"
		if not self.cache.contains(key, hash):
			data = self.cdn_get("/config/{0}/{1}/{2}".format(hash[0:2], hash[2:4], hash))
			self.cache.write(key, hash, data)
		else:
			data = self.cache.get(key, hash)
		config = RawConfigParser()
		config.readfp(StringIO("[DEFAULT]" + data.decode("utf-8")))
		return dict(config["DEFAULT"])

	def cdn_get(self, path):
		r = requests.get(self.cdn + path)
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
		print(v)


if __name__ == "__main__":
	main()
