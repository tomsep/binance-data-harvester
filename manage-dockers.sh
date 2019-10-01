#!/bin/bash

source setup-vars.sh

if [ -z $1 ] 
then
	echo "Subcommands: build | init | continue | stop | log"
	exit 1
fi

if ! [ -z $2 ]
then
	echo "Script takes only one argument"
	exit 1
fi


if [ $1 = build ]
then
  docker build -f Dockerfile_mysql . --tag $DB_CONTAINER_NAME
	docker build -f Dockerfile_app . --tag $APP_CONTAINER_NAME
	exit 1
fi

if [ $1 = init ]
then


	if [ -z $DB_DATA_PATH ]  # test emptiness
	then
		docker run -p 3306:3306 --name $DB_CONTAINER_NAME -e MYSQL_ROOT_PASSWORD=$DB_ROOT_PASSWORD \
		-d $DB_CONTAINER_NAME
	else
		docker run -p 3306:3306 --name $DB_CONTAINER_NAME -e MYSQL_ROOT_PASSWORD=$DB_ROOT_PASSWORD \
		-d -v $DB_DATA_PATH:/var/lib/mysql $DB_CONTAINER_NAME
	fi
	sleep 3
	docker run --network host --name $APP_CONTAINER_NAME -d $APP_CONTAINER_NAME
  exit 1
fi


if [ $1 = continue ]
then
	docker start $DB_CONTAINER_NAME
	docker start $APP_CONTAINER_NAME
	exit 1
fi


if [ $1 = stop ]
then
	docker stop $APP_CONTAINER_NAME
	docker stop $DB_CONTAINER_NAME
	exit 1
fi

if [ $1 = log ]
then
  docker logs -f $APP_CONTAINER_NAME
  exit 1
fi

echo "Unrecognized command: $1"
