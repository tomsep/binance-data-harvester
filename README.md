![](https://img.shields.io/badge/python-3.7-blue) ![](https://img.shields.io/badge/license-MIT-green) 

Streams [Binance](https://www.binance.com) ticker and order book data to a local SQL database.
Logged data may reach 10 GB/day and beyond depending on the number of trading pairs logged.

12H results MB
asks+bids, orderbook, ticker (about 42k rows each)
(15.5*2 + 1.5 + 2.5)
~= 70 MB / day per one symbol




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

## Other useful notes
* Access shell within the mysql container

		docker exec -it <container_name> bash -l
		# SQL execution shell:
		mysql --user=<user> --password <db_name>
		
/home/tomi/mydb_tmp:/var/lib/mysql
	
	# Build containers
	docker build -f Dockerfile_mysql . --tag harvester_db
	docker build -f Dockerfile_app . --tag harvester
	
	# Running (only once, next time use docker start)
	# To specify custom db data path use arg (-v <desired_path>:/var/lib/mysql)
	docker run -p 3306:3306 --name harvester_db -e MYSQL_ROOT_PASSWORD=root -d harvester_db
	
	docker run --network host --name harvester -d harvester
	
	# Stopping
	docker stop harvester
	docker stop harvester_db
	
	# Starting
	docker start harvester_db
	docker start harvester
	
	# Removing (-v removes volumes)
	docker rm <name> -v


https://hub.docker.com/r/mysql/mysql-server/
docker exec -it mysql1 mysql -uroot -p


GRANT ALL PRIVILEGES ON *.* TO 'username'@'localhost' IDENTIFIED BY 'password';

