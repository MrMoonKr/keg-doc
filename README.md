# keg
[![Build Status](https://api.travis-ci.org/HearthSim/keg.svg?branch=master)](https://travis-ci.org/HearthSim/keg)

A library and client implementation of Blizzard's NGDP protocol.  
NGDP = TACT for Web CDN + CASC for Local HDD

## 용어정리

- NGDP : Next Generation Distribution Platform.
- TACT : Trustted Application Content Transfer.
- CASC : Content Addressable Storage Container. 

## 개발환경
- Python 3.10.1
- setuptools = setup.py + setup.cfg
- venv
```
$ python -m venv venv-keg-doc
$ ./venv-keg-doc/Scripts/Activate.ps1
$ pip install .

$ deactivate
```


## Installation

Python 3.6+ only.  
To install dependencies, go to the `keg` root directory and run `pip install`.  
Installing keg will install a `ngdp` program to your path.


## Usage

### Initialize a repository

**To create a Keg repository in the current directory, run the command: `ngdp init`.**

This creates a `.ngdp` folder in that directory, with the following structure:
 - `.ngdp/keg.conf`: A TOML configuration file for Keg
 - `.ngdp/keg.db`: A sqlite3 database used as a cache for various HTTP responses.
 - `.ngdp/objects/`: A directory which contains all repository objects (the Object Store).
 - `.ngdp/responses/`: A directory which contains all stateful HTTP responses.


### Remotes

**To add a remote, run the command: `ngdp remote add {url}`.**

Much like Git, ngdp has a concept of remotes.
A remote is a URL to a NGDP repository.  Currently, remotes are always read-only.

Blizzard's remotes always look like: `http://{REGION}.patch.battle.net:1119/{PATH}`,
where `{REGION}` is a region such as `eu`, `us`, `public-test`, etc and `{PATH}`
is the path to the repository (such as `tpr/hs`).

*Think of those like git repositories on Github: the path to the repository on the
HTTP server is not part of the git protocol.*

For convenience, whenever specifying a remote on the command line, Keg prefixes
it with a default prefix. This is by default set to `http://us.patch.battle.net:1119/`
and can be changed in `keg.conf` (`keg.default-remote-prefix`).

That means the following two commands are, by default, equivalent:

- `ngdp remote add hsb`
- `ngdp remote add http://us.patch.battle.net:1119/hsb`


### The Object Store

Similarly to Git, objects are stored in `.ngdp/objects`.
That directory replicates 1:1 upstream CDNs for NGDP data.

It contains the following types of files:

 - CDN and Build configuration files (`config/`)
 - Product configuration files (`configs/data/`)
 - Packed and loose files (`data/`), and their indices (`data/.../*.index`)
 - Patch data (`patch/`) and patch indices (`patch/.../*.index`)


### Fetching and Installing data

**To fetch data for a remote, run `ngdp fetch {remote}`.**
**To fetch data for all remotes, run `ngdp fetch-all`.**

Again taking inspiration from git, Keg separates the concepts of the downloaded
data files, and the "checked out" (installed) files.

A repository may contain several concurrent builds. Different builds deployed to
different platforms (mobile vs. desktop) or regions (US vs. China), for example.
On top of this, within a particular build, not all files are relevant for all
installations.

When Keg fetches a remote, it fetches *all* that information, unless told otherwise.
Once that information has been fetched on disk, it is possible to install a build.

**To install a build, run `ngdp install {remote} {version} {outdir}`.**

In that command, the "version" can be a Build Name (eg. `12.0.0.26080`), Build ID
(`26080`), or Build Config key (`360b92e813c1eb6ac1941fcca0c51f85`).

If not specified, `outdir` defaults to `.`.

**To view all builds for a remote, run `ngdp inspect {remote}`.**

Without any arguments, the `install` command will install every file that is part
of the build. Builds however are usually filtered (eg. by Platform or Language).

For example, to replicate a Production, English, Windows installation of Hearthstone,
you would run: `ngdp install hsb {version} --tags Windows --tags enUS --tags Production out/`.

You may also run the command with `--dry-run` to show what would get installed.

**To display a list of tags, run `ngdp install --show-tags {remote} {version}`.**


### Integrity checking

By default, Keg will aggressively verify integrity whenever it can.
This behaviour is defined in the `keg.verify-integrity` key of the `keg.conf` file.

When encountering an integrity error, Keg will immediately abort and exit.

Keg also supports verifying the integrity of the repository.

**To run a full integrity check, run `ngdp fsck`.**

Run that command with the `--delete` argument if you wish to delete bad files.


### Archives

Archives are large files which are concatenations of lots of small BLTE fragments.
They are stored in `objects/data/.../*` and always should have a corresponding `.index`
file.
Files in the `data/` folder which do not have a corresponding `.index` file should always
be loose files, referenced by encoding files.

- To list all archives, run `ngdp archive list`, optionally with a `--remote` argument.
  White keys are available locally, dark keys are not.
- To extract an archive, run `ngdp archive extract <key>`.
- To create an archive from extracted fragments, run `ngdp archive create <key> [folder]`.
  The folder must contain all the required fragment. The index must already exist.


## Concepts

### What is NGDP?

NGDP ("Next-Generation Data Pipeline") is a protocol over HTTP which allows the
indexing and retrieval of content-addressed files.

The protocol supports:
 - Multiple repository remotes (conceptually, a "program")
 - Mirroring a remote across multiple endpoints ("CDNs")
 - Versioning: Per-version filelists, with shared files across versions
 - Named endpoints with their own file lists and configuration (eg. "bgdl")
 - Arbitrary metadata per version
 - Individual file distribution
 - Aggregation of small files within larger archives
 - Individual file addressing within larger archives
 - Encryption, including encryption key distribution
 - End-to-end file integrity checks (md5)


### What is BLTE?

BLTE ("BLock Table Encoding") is a compression/encryption container that
supports chunking data.

Keg includes a `blte` binary utility to deal with BLTE files:

 - `blte extract` (or `blte x`): Decompresses and extracts a BLTE file.
 - `blte verify`: Verifies a BLTE file against its filename key.
 - `blte fix`: Fixes BLTE files with extraneous data at the end.
 - `blte dump`: Dumps the block table of the BLTE file, for debugging purposes.


### What is CASC?

...


## License

This project is licensed under the terms of the MIT license.
See the LICENSE file for the full license text.


## Acknowledgements

Most of this work would not be possible without the help of
[Robert Nix](https://twitter.com/mischanix), [Marlamin](https://twitter.com/Marlamin)
and [ModoX](https://github.com/mdX7/) as well as the fine folks who maintain
the [WoWDev Wiki](https://wowdev.wiki/),
