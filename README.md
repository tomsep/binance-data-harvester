![](https://img.shields.io/badge/python-3.7-blue) ![](https://img.shields.io/badge/license-MIT-green) 

Streams [Binance](https://www.binance.com) ticker and order book data to a local SQL database.
Logged data may reach 10 GB/day and beyond depending on the number of trading pairs logged.



## Features

* Log 24hr ticker and order books for selected trading pairs.
* Batched disk write to reduce total disk writes.
* Docker container deployment.

## Usage

Rename *conf_template.yml* to *conf.yml*. Edit values if needed.
Do not edit db_path variable if you plan to run the program inside Docker container.
For container the database location is specified using the -v parameter.


	# Build docker container
	cd /project_source_root/
	docker build --tag=binance-data-harvester .
	
	# Run
	# Replace "~/home/data" with your database folder path.
	docker run -it -v ~/home/data:/app/data binance-data-harvester
	
	# OR
	# To map to "./data" use "$PWD/data"
	docker run -it -v $PWD/data:/app/data binance-data-harvester
	
## Planned features
* Merge data from multiple program instances (redundancy).
* Data compression.
* Status server.
* Ability to recover from power or network failures.