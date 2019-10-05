![](https://img.shields.io/badge/python-3.7-blue) ![](https://img.shields.io/badge/license-MIT-green) 


Streams [Binance](https://www.binance.com) ticker and order book data to a local SQLite database. Disk space usage daily is approximately 26 MB per trading pair (with order book depth 5), e.g. recording everyhting would take around 5 GB/day.


## Features

* Log 24hr ticker and order books (depth 5) for selected trading pairs.
* Docker container deployment.
* Hot unplug target drive (inserts are queued in memory until drive is mounted back).
* SQLite database for best compatibility and portability.

## Usage

Rename *conf_template.yml* to *conf.yml*. Edit values if needed.
Set db_dir to '/data/' if you plan to run the program inside Docker container. For container the database location is specified using the -v parameter.


	# Build docker container
	cd /project_source_root/
	docker build --tag=harvester .
	
	# Run
	# Replace "~/home/data" with your database folder path.
	docker run -it -v ~/home/data:/data harvester
	
	# OR for example
	# To map to "./data" use "$PWD/data"
	docker run -it -v $PWD/data:/data harvester
	
## Planned features
* Merge data from multiple program instances (redundancy).
* Data compression.
* Status server.
* Ability to recover from power or network failures.

## Other notes
* SQLite is set to use [WAL](https://www.sqlite.org/wal.html) journal mode to reduce disk writes.

* Foreign keys are enabled for SQLite.