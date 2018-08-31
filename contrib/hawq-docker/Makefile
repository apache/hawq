#!/usr/bin/make all

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

THIS_MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
TOP_DIR := $(abspath $(dir ${THIS_MAKEFILE_PATH}))
NDATANODES := 3
CUR_DATANODE := 1
OS_VERSION := centos7
# Do not use underscore "_" in CLUSTER_ID
CLUSTER_ID := $(OS_VERSION)
# Monut this local directory to /data in data container and share with other containers
LOCAL :=
# networks used in docker
NETWORK := $(CLUSTER_ID)_hawq_network
HAWQ_HOME := "/data/hawq-devel"
PXF_CLASSPATH_TEMPLATE = "hdp"
JAVA_TOOL_OPTIONS := -Dfile.encoding=UTF8

all:
	@echo " Usage:"
	@echo "    To setup a build and test environment:         make run"
	@echo "    To start all containers:                       make start"
	@echo "    To stop all containers:                        make stop"
	@echo "    To remove hdfs containers:                     make clean"
	@echo "    To remove all containers:                      make distclean"
	@echo ""
	@echo "    To build images locally:                       make build-image"
	@echo "    To pull latest images:                         make pull"
	@echo ""
	@echo "    To build Hawq runtime:                         make build-hawq"
	@echo "    To initialize Hawq on Namenode:                make init-hawq"
	@echo "    To start Hawq on Namenode:                     make start-hawq"
	@echo "    To stop Hawq on Namenode:                      make stop-hawq"
	@echo "    To check Hawq status on Namenode:              make status-hawq"
	@echo "    To build PXF runtime:                          make build-pxf"
	@echo "    To initialize PXF on Namenode/Datanodes:       make init-pxf"
	@echo "    To start PXF on Namenode/Datanodes:            make start-pxf"
	@echo "    To stop PXF on on Namenode/Datanodes:          make stop-hawq"
	@echo "    To check PXF status on Namenode/Datanodes:     make status-pxf"

build-image:
	@make -f $(THIS_MAKEFILE_PATH) build-hawq-dev-$(OS_VERSION)
	@make -f $(THIS_MAKEFILE_PATH) build-hawq-test-$(OS_VERSION)
	@echo "Build Images Done!"

build-hawq-dev-$(OS_VERSION): $(TOP_DIR)/$(OS_VERSION)-docker/hawq-dev/Dockerfile
	@echo build hawq-dev:$(OS_VERSION) image
	docker build -t hawq/hawq-dev:$(OS_VERSION) $(TOP_DIR)/$(OS_VERSION)-docker/hawq-dev/

build-hawq-test-$(OS_VERSION): $(TOP_DIR)/$(OS_VERSION)-docker/hawq-test/Dockerfile
	@echo build hawq-test:$(OS_VERSION) image
	docker build \
		--build-arg=PXF_CLASSPATH_TEMPLATE="`cat ../../pxf/pxf-service/src/configs/templates/pxf-private-${PXF_CLASSPATH_TEMPLATE}.classpath.template`" \
		--build-arg=PXF_LOG4J_PROPERTIES="`cat ../../pxf/pxf-service/src/main/resources/pxf-log4j.properties`" \
		-t hawq/hawq-test:$(OS_VERSION) $(TOP_DIR)/$(OS_VERSION)-docker/hawq-test/

create-data-container:
	@echo create ${CLUSTER_ID}-data container
	@if [ ! -z "$(LOCAL)" -a ! -d "$(LOCAL)" ]; then \
		echo "LOCAL must be set to a directory!"; \
		exit 1; \
	fi
	@if [ -z "`docker ps -a --filter="name=${CLUSTER_ID}-data$$" | grep -v CONTAINER`" ]; then \
		if [ -z "$(LOCAL)" ]; then \
			docker create -v /data --name=${CLUSTER_ID}-data hawq/hawq-dev:$(OS_VERSION) /bin/true; \
		else \
			docker create -v $(LOCAL):/data --name=${CLUSTER_ID}-data hawq/hawq-dev:$(OS_VERSION) /bin/true; \
		fi \
	else \
		echo "${CLUSTER_ID}-data container already exist!"; \
	fi

run:
	@if [ -z "`docker network ls 2>/dev/null`" ]; then \
 		make -f $(THIS_MAKEFILE_PATH) NETWORK=default create-data-container && \
		make -f $(THIS_MAKEFILE_PATH) NETWORK=default run-hdfs; \
	else \
		if [ -z "`docker network ls 2>/dev/null | grep $(NETWORK)`" ]; then \
			echo create network $(NETWORK) && \
			docker network create --driver bridge $(NETWORK); \
		fi && \
		make -f $(THIS_MAKEFILE_PATH) create-data-container && \
		make -f $(THIS_MAKEFILE_PATH) run-hdfs; \
	fi

run-hdfs:
	@make -f $(THIS_MAKEFILE_PATH) run-namenode-container
	@i=1; \
	while [ $$i -le $(NDATANODES) ] ; do \
		make -f $(THIS_MAKEFILE_PATH) CUR_DATANODE=$$i run-datanode-container; \
		i=$$((i+1)); \
	done
	@echo "HAWQ Environment Setup Done!"
	@echo 'run "docker exec -it ${CLUSTER_ID}-namenode bash" to attach to ${CLUSTER_ID}-namenode node'

run-namenode-container:
	@echo "run ${CLUSTER_ID}-namenode container"
	@if [ -z "`docker ps -a --filter="name=${CLUSTER_ID}-namenode" | grep -v CONTAINER`" ]; then \
		docker run --privileged -itd --net=$(NETWORK) --hostname=${CLUSTER_ID}-namenode --name=${CLUSTER_ID}-namenode \
			--volumes-from ${CLUSTER_ID}-data hawq/hawq-test:$(OS_VERSION); \
	else \
		echo "${CLUSTER_ID}-namenode container already exist!"; \
	fi

run-datanode-container:
	@echo "run ${CLUSTER_ID}-datanode$(CUR_DATANODE) container"
	@if [ -z "`docker ps -a --filter="name=${CLUSTER_ID}-datanode$(CUR_DATANODE)" | grep -v CONTAINER`" ]; then \
		docker run --privileged -itd --net=$(NETWORK) --hostname=${CLUSTER_ID}-datanode$(CUR_DATANODE) \
			--name=${CLUSTER_ID}-datanode$(CUR_DATANODE) -e NAMENODE=${CLUSTER_ID}-namenode \
			--volumes-from ${CLUSTER_ID}-data hawq/hawq-test:$(OS_VERSION); \
	else \
		echo "${CLUSTER_ID}-datanode$(CUR_DATANODE) container already exist!"; \
	fi

start:
	@make -f $(THIS_MAKEFILE_PATH) start-hdfs
	@echo 'run "docker exec -it ${CLUSTER_ID}-namenode bash" to attach to ${CLUSTER_ID}-namenode node'

start-hdfs:
	@make -f $(THIS_MAKEFILE_PATH) start-namenode-container
	@i=1;\
	while [ $$i -le $(NDATANODES) ] ; do \
		make -f $(THIS_MAKEFILE_PATH) CUR_DATANODE=$$i start-datanode-container; \
		i=$$((i+1)); \
	done
	@echo "Start All Containers Done!"

start-namenode-container:
	@echo "start ${CLUSTER_ID}-namenode container"
	@if [ ! -z "`docker ps -a --filter="name=${CLUSTER_ID}-namenode" | grep -v CONTAINER`" ]; then \
		docker start ${CLUSTER_ID}-namenode; \
	else \
		echo "${CLUSTER_ID}-namenode container does not exist!" && exit 1; \
	fi

start-datanode-container:
	@echo "start ${CLUSTER_ID}-datanode$(CUR_DATANODE) container"
	@if [ ! -z "`docker ps -a --filter="name=${CLUSTER_ID}-datanode$(CUR_DATANODE)" | grep -v CONTAINER`" ]; then \
		docker start ${CLUSTER_ID}-datanode$(CUR_DATANODE); \
	else \
		echo "${CLUSTER_ID}-datanode$(CUR_DATANODE) container does not exist!" && exit 1; \
	fi

stop:
	@make -f $(THIS_MAKEFILE_PATH) stop-hdfs

stop-hdfs:
	@make -f $(THIS_MAKEFILE_PATH) stop-namenode-container
	@i=1; \
	while [ $$i -le $(NDATANODES) ] ; do \
		make -f $(THIS_MAKEFILE_PATH) CUR_DATANODE=$$i stop-datanode-container; \
		i=$$((i+1)); \
	done
	@echo "Stop All Containers Done!"

stop-namenode-container:
	@echo "stop ${CLUSTER_ID}-namenode container"
	@if [ ! -z "`docker ps -a --filter="name=${CLUSTER_ID}-namenode" | grep -v CONTAINER`" ]; then \
		docker stop -t 0 ${CLUSTER_ID}-namenode; \
	else \
		echo "${CLUSTER_ID}-namenode container does not exist!"; \
	fi

stop-datanode-container:
	@echo "stop ${CLUSTER_ID}-datanode$(CUR_DATANODE) container"
	@if [ ! -z "`docker ps -a --filter="name=${CLUSTER_ID}-datanode$(CUR_DATANODE)" | grep -v CONTAINER`" ]; then \
		docker stop -t 0 ${CLUSTER_ID}-datanode$(CUR_DATANODE); \
	else \
		echo "${CLUSTER_ID}-datanode$(CUR_DATANODE) container does not exist!"; \
	fi

remove-hdfs:
	@make -f $(THIS_MAKEFILE_PATH) remove-namenode-container
	@i=1; \
	while [ $$i -le $(NDATANODES) ] ; do \
		make -f $(THIS_MAKEFILE_PATH) CUR_DATANODE=$$i remove-datanode-container; \
		i=$$((i+1)); \
	done
	@echo "Remove HDFS Done!"

remove-namenode-container:
	@echo "make ${CLUSTER_ID}-namenode container"
	@if [ ! -z "`docker ps -a --filter="name=${CLUSTER_ID}-namenode" | grep -v CONTAINER`" ]; then \
		docker rm -v ${CLUSTER_ID}-namenode; \
	else \
		echo "${CLUSTER_ID}-namenode container does not exist!"; \
	fi

remove-datanode-container:
	@echo "make ${CLUSTER_ID}-datanode$(CUR_DATANODE) container"
	@if [ ! -z "`docker ps -a --filter="name=${CLUSTER_ID}-datanode$(CUR_DATANODE)" | grep -v CONTAINER`" ]; then \
		docker rm -v ${CLUSTER_ID}-datanode$(CUR_DATANODE); \
	else \
		echo "${CLUSTER_ID}-datanode$(CUR_DATANODE) container does not exist!"; \
	fi

remove-data:
	@echo remove ${CLUSTER_ID}-data container
	@if [ ! -z "`docker ps -a --filter="name=${CLUSTER_ID}-data" | grep -v CONTAINER`" ]; then \
		docker rm -v ${CLUSTER_ID}-data; \
	else \
		echo "${CLUSTER_ID}-data container does not exist!"; \
	fi

pull:
	@echo latest images
	#There is no hawq repo in docker.io currently, we just build up a local repo to mimic the docker registry here.
	#For remote registry.
	#docker pull hawq/hawq-dev:$(OS_VERSION)
	#docker pull hawq/hawq-test:$(OS_VERSION)
	#For local registry, user need to install local registry and push images before following steps.
	docker pull localhost:5000/hawq-dev:$(OS_VERSION)
	docker pull localhost:5000/hawq-test:$(OS_VERSION)
	docker tag localhost:5000/hawq-dev:$(OS_VERSION) hawq/hawq-dev:$(OS_VERSION)
	docker tag localhost:5000/hawq-test:$(OS_VERSION) hawq/hawq-test:$(OS_VERSION)

clean:
	@make -f $(THIS_MAKEFILE_PATH) stop 2>&1 >/dev/null || true
	@make -f $(THIS_MAKEFILE_PATH) remove-hdfs 2>&1 >/dev/null || true
	@echo "Clean Done!"

distclean:
	@make -f $(THIS_MAKEFILE_PATH) stop 2>&1 >/dev/null || true
	@make -f $(THIS_MAKEFILE_PATH) remove-hdfs 2>&1 >/dev/null || true
	@make -f $(THIS_MAKEFILE_PATH) remove-data 2>&1 >/dev/null || true
	@if [ ! -z "`docker network ls 2>/dev/null | grep $(NETWORK)`" ]; then \
		echo remove network $(NETWORK); \
		docker network rm $(NETWORK) 2>&1 >/dev/null || true; \
	fi
	@echo "Distclean Done!"

build-hawq:
	@echo "Make sure you have executed make build-image"
	@echo "Make sure you have executed make run"
	@echo "Logging into ${CLUSTER_ID}-namenode container"
	@if [ ! -z "`docker ps -a --filter="name=${CLUSTER_ID}-namenode" | grep -v CONTAINER`" ]; then \
		docker exec \
			-e "HAWQ_HOME=$(HAWQ_HOME)" \
			-e "JAVA_TOOL_OPTIONS=$(JAVA_TOOL_OPTIONS)" \
			-u gpadmin --privileged -it ${CLUSTER_ID}-namenode /bin/bash -c "service-hawq.sh --build"; \
	else \
		echo "${CLUSTER_ID}-namenode container does not exist!"; \
	fi

init-hawq:
	@echo "Make sure you have executed make build-hawq"
	@echo "Logging into ${CLUSTER_ID}-namenode container"
	@if [ ! -z "`docker ps -a --filter="name=${CLUSTER_ID}-namenode" | grep -v CONTAINER`" ]; then \
		docker exec \
			-e "HAWQ_HOME=$(HAWQ_HOME)" \
			-e "NAMENODE=${CLUSTER_ID}-namenode" \
			-u gpadmin --privileged -it ${CLUSTER_ID}-namenode /bin/bash -c "service-hawq.sh --init"; \
	else \
		echo "${CLUSTER_ID}-namenode container does not exist!"; \
	fi

start-hawq:
	@echo "Make sure you have executed make init-hawq"
	@echo "Logging into ${CLUSTER_ID}-namenode container"
	@if [ ! -z "`docker ps -a --filter="name=${CLUSTER_ID}-namenode" | grep -v CONTAINER`" ]; then \
		docker exec \
			-e "HAWQ_HOME=$(HAWQ_HOME)" \
			-e "JAVA_TOOL_OPTIONS=$(JAVA_TOOL_OPTIONS)" \
			-u gpadmin --privileged -it ${CLUSTER_ID}-namenode /bin/bash -c "service-hawq.sh --start"; \
	else \
		echo "${CLUSTER_ID}-namenode container does not exist!"; \
	fi

stop-hawq:
	@echo "Logging into ${CLUSTER_ID}-namenode container"
	@if [ ! -z "`docker ps -a --filter="name=${CLUSTER_ID}-namenode" | grep -v CONTAINER`" ]; then \
		docker exec \
			-e "HAWQ_HOME=$(HAWQ_HOME)" \
			-e "JAVA_TOOL_OPTIONS=$(JAVA_TOOL_OPTIONS)" \
			-u gpadmin --privileged -it ${CLUSTER_ID}-namenode /bin/bash -c "service-hawq.sh --stop"; \
	else \
		echo "${CLUSTER_ID}-namenode container does not exist!"; \
	fi

status-hawq:
	@echo "Logging into ${CLUSTER_ID}-namenode container"
	@if [ ! -z "`docker ps -a --filter="name=${CLUSTER_ID}-namenode" | grep -v CONTAINER`" ]; then \
		docker exec \
			-e "HAWQ_HOME=$(HAWQ_HOME)" \
			-e "JAVA_TOOL_OPTIONS=$(JAVA_TOOL_OPTIONS)" \
			-e  "USER=gpadmin" \
			-u gpadmin --privileged -it ${CLUSTER_ID}-namenode /bin/bash -c "service-hawq.sh --status"; \
	else \
		echo "${CLUSTER_ID}-namenode container does not exist!"; \
	fi

build-pxf:
	@echo "Make sure you have executed make build-image"
	@echo "Make sure you have executed make run"
	@make -f $(THIS_MAKEFILE_PATH) pxf-namenode
	@i=1; \
	while [ $$i -le $(NDATANODES) ] ; do \
		make -f $(THIS_MAKEFILE_PATH) CUR_DATANODE=$$i pxf-datanode; \
		i=$$((i+1)); \
	done

pxf-namenode:
	@echo "Logging into ${CLUSTER_ID}-namenode container"
	@if [ ! -z "`docker ps -a --filter="name=${CLUSTER_ID}-namenode" | grep -v CONTAINER`" ]; then \
		docker exec \
			-e "HAWQ_HOME=$(HAWQ_HOME)" \
			-e "JAVA_TOOL_OPTIONS=$(JAVA_TOOL_OPTIONS)" \
			-e "PXF_CLASSPATH_TEMPLATE=$(PXF_CLASSPATH_TEMPLATE)" \
			-u gpadmin --privileged -it ${CLUSTER_ID}-namenode /bin/bash -c "service-pxf.sh --build"; \
	else \
		echo "${CLUSTER_ID}-namenode container does not exist!" && exit 1; \
	fi

pxf-datanode:
	@echo "Logging into ${CLUSTER_ID}-datanode$(CUR_DATANODE) container"
	@if [ ! -z "`docker ps -a --filter="name=${CLUSTER_ID}-datanode$(CUR_DATANODE)" | grep -v CONTAINER`" ]; then \
		docker exec \
			-e "HAWQ_HOME=$(HAWQ_HOME)" \
			-e "JAVA_TOOL_OPTIONS=$(JAVA_TOOL_OPTIONS)" \
			-u gpadmin --privileged -it ${CLUSTER_ID}-datanode$(CUR_DATANODE) /bin/bash -c "service-pxf.sh --build"; \
	else \
		echo "${CLUSTER_ID}-datanode$(CUR_DATANODE) container does not exist!" && exit 1; \
	fi

init-pxf:
	@echo "Make sure you have executed make build-pxf"
	@make -f $(THIS_MAKEFILE_PATH) init-pxf-namenode
	@i=1; \
	while [ $$i -le $(NDATANODES) ] ; do \
		make -f $(THIS_MAKEFILE_PATH) CUR_DATANODE=$$i init-pxf-datanode; \
		i=$$((i+1)); \
	done

init-pxf-namenode:
	@echo "Logging into ${CLUSTER_ID}-namenode container"
	@if [ ! -z "`docker ps -a --filter="name=${CLUSTER_ID}-namenode" | grep -v CONTAINER`" ]; then \
		docker exec \
			-e "HAWQ_HOME=$(HAWQ_HOME)" \
			-e "NAMENODE=${CLUSTER_ID}-namenode" \
			-u gpadmin --privileged -it ${CLUSTER_ID}-namenode /bin/bash -c "service-pxf.sh --init"; \
	else \
		echo "${CLUSTER_ID}-namenode container does not exist!" && exit 1; \
	fi

init-pxf-datanode:
	@echo "Logging into ${CLUSTER_ID}-datanode$(CUR_DATANODE) container"
	@if [ ! -z "`docker ps -a --filter="name=${CLUSTER_ID}-datanode$(CUR_DATANODE)" | grep -v CONTAINER`" ]; then \
		docker exec \
			-e "HAWQ_HOME=$(HAWQ_HOME)" \
			-e "NAMENODE=${CLUSTER_ID}-namenode" \
			-u gpadmin --privileged -it ${CLUSTER_ID}-datanode$(CUR_DATANODE) /bin/bash -c "service-pxf.sh --init"; \
	else \
		echo "${CLUSTER_ID}-datanode$(CUR_DATANODE) container does not exist!" && exit 1; \
	fi

start-pxf:
	@echo "Make sure you have executed make init-pxf"
	@make -f $(THIS_MAKEFILE_PATH) start-pxf-namenode
	@i=1; \
	while [ $$i -le $(NDATANODES) ] ; do \
		make -f $(THIS_MAKEFILE_PATH) CUR_DATANODE=$$i start-pxf-datanode; \
		i=$$((i+1)); \
	done

start-pxf-namenode:
	@echo "Logging into ${CLUSTER_ID}-namenode container"
	@if [ ! -z "`docker ps -a --filter="name=${CLUSTER_ID}-namenode" | grep -v CONTAINER`" ]; then \
		docker exec \
			-e "HAWQ_HOME=$(HAWQ_HOME)" \
			-e "NAMENODE=${CLUSTER_ID}-namenode" \
			-u gpadmin --privileged -it ${CLUSTER_ID}-namenode /bin/bash -c "service-pxf.sh --start"; \
	else \
		echo "${CLUSTER_ID}-namenode container does not exist!" && exit 1; \
	fi

start-pxf-datanode:
	@echo "Logging into ${CLUSTER_ID}-datanode$(CUR_DATANODE) container"
	@if [ ! -z "`docker ps -a --filter="name=${CLUSTER_ID}-datanode$(CUR_DATANODE)" | grep -v CONTAINER`" ]; then \
		docker exec \
			-e "HAWQ_HOME=$(HAWQ_HOME)" \
			-e "NAMENODE=${CLUSTER_ID}-namenode" \
			-u gpadmin --privileged -it ${CLUSTER_ID}-datanode$(CUR_DATANODE) /bin/bash -c "service-pxf.sh --start"; \
	else \
		echo "${CLUSTER_ID}-datanode$(CUR_DATANODE) container does not exist!" && exit 1; \
	fi

stop-pxf:
	@make -f $(THIS_MAKEFILE_PATH) stop-pxf-namenode
	@i=1; \
	while [ $$i -le $(NDATANODES) ] ; do \
		make -f $(THIS_MAKEFILE_PATH) CUR_DATANODE=$$i stop-pxf-datanode; \
		i=$$((i+1)); \
	done

stop-pxf-namenode:
	@echo "Logging into ${CLUSTER_ID}-namenode container"
	@if [ ! -z "`docker ps -a --filter="name=${CLUSTER_ID}-namenode" | grep -v CONTAINER`" ]; then \
		docker exec \
			-e "HAWQ_HOME=$(HAWQ_HOME)" \
			-e "NAMENODE=${CLUSTER_ID}-namenode" \
			-u gpadmin --privileged -it ${CLUSTER_ID}-namenode /bin/bash -c "service-pxf.sh --stop"; \
	else \
		echo "${CLUSTER_ID}-namenode container does not exist!" && exit 1; \
	fi

stop-pxf-datanode:
	@echo "Logging into ${CLUSTER_ID}-datanode$(CUR_DATANODE) container"
	@if [ ! -z "`docker ps -a --filter="name=${CLUSTER_ID}-datanode$(CUR_DATANODE)" | grep -v CONTAINER`" ]; then \
		docker exec \
			-e "HAWQ_HOME=$(HAWQ_HOME)" \
			-e "NAMENODE=${CLUSTER_ID}-namenode" \
			-u gpadmin --privileged -it ${CLUSTER_ID}-datanode$(CUR_DATANODE) /bin/bash -c "service-pxf.sh --stop"; \
	else \
		echo "${CLUSTER_ID}-datanode$(CUR_DATANODE) container does not exist!" && exit 1; \
	fi

status-pxf:
	@make -f $(THIS_MAKEFILE_PATH) status-pxf-namenode
	@i=1; \
	while [ $$i -le $(NDATANODES) ] ; do \
		make -f $(THIS_MAKEFILE_PATH) CUR_DATANODE=$$i status-pxf-datanode; \
		i=$$((i+1)); \
	done

status-pxf-namenode:
	@echo "Logging into ${CLUSTER_ID}-namenode container"
	@if [ ! -z "`docker ps -a --filter="name=${CLUSTER_ID}-namenode" | grep -v CONTAINER`" ]; then \
		docker exec \
			-e "HAWQ_HOME=$(HAWQ_HOME)" \
			-e "NAMENODE=${CLUSTER_ID}-namenode" \
			-u gpadmin --privileged -it ${CLUSTER_ID}-namenode /bin/bash -c "service-pxf.sh --status"; \
	else \
		echo "${CLUSTER_ID}-namenode container does not exist!" && exit 1; \
	fi

status-pxf-datanode:
	@echo "Logging into ${CLUSTER_ID}-datanode$(CUR_DATANODE) container"
	@if [ ! -z "`docker ps -a --filter="name=${CLUSTER_ID}-datanode$(CUR_DATANODE)" | grep -v CONTAINER`" ]; then \
		docker exec \
			-e "HAWQ_HOME=$(HAWQ_HOME)" \
			-e "NAMENODE=${CLUSTER_ID}-namenode" \
			-u gpadmin --privileged -it ${CLUSTER_ID}-datanode$(CUR_DATANODE) /bin/bash -c "service-pxf.sh --status"; \
	else \
		echo "${CLUSTER_ID}-datanode$(CUR_DATANODE) container does not exist!" && exit 1; \
	fi