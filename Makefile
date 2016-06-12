.DEFAULT_GOAL := help

test:
	./rebar skip_deps=true eunit

bootstrap: # set up environment
	./rebar get-deps

.PHONY: test bootstrap

test-environment: zookeeper mesos-master mesos-slave # install integration environment

test-environment-down: # remove integration environment
	docker rm -f zookeeper mesos-master mesos-slave

zookeeper:
	docker run -d --net=host --name zookeeper netflixoss/exhibitor:1.5.2

mesos-master:
	docker run -d --net=host --name mesos-master \
	-e MESOS_PORT=5050 \
	-e MESOS_ZK=zk://127.0.0.1:2181/mesos \
	-e MESOS_QUORUM=1 \
	-e MESOS_REGISTRY=in_memory \
	-e MESOS_LOG_DIR=/var/log/mesos \
	-e MESOS_WORK_DIR=/var/tmp/mesos \
	-e HOSTNAME=dev \
	-v "$(pwd)/log/mesos:/var/log/mesos" \
	-v "$(pwd)/tmp/mesos:/var/tmp/mesos" \
	-it --entrypoint mesos-master \
	mesosphere/mesos:0.28.1 --ip=127.0.0.1 --hostname=127.0.0.1 --registry=in_memory

mesos-slave:
	docker run -d --net=host --name mesos-slave --privileged \
	-e MESOS_PORT=5051 \
	-e MESOS_MASTER=zk://127.0.0.1:2181/mesos \
	-e MESOS_SWITCH_USER=0 \
	-e MESOS_CONTAINERIZERS=docker,mesos \
	-e MESOS_LOG_DIR=/var/log/mesos \
	-e MESOS_WORK_DIR=/var/tmp/mesos \
	-v "$(pwd)/log/mesos-slave:/var/log/mesos" \
	-v "$(pwd)/tmp/mesos-slave:/var/tmp/mesos" \
	-v /var/run/docker.sock:/var/run/docker.sock \
	-v /cgroup:/cgroup \
	-v /sys:/sys \
	-v "/usr/bin/docker:/usr/local/bin/docker" \
	-v /lib/x86_64-linux-gnu/libsystemd-journal.so.0:/lib/x86_64-linux-gnu/libsystemd-journal.so.0 \
	-v /usr/lib/x86_64-linux-gnu/libapparmor.so.1:/usr/lib/x86_64-linux-gnu/libapparmor.so.1 \
	-v /usr/lib/x86_64-linux-gnu/libltdl.so.7:/usr/lib/x86_64-linux-gnu/libltdl.so.7 \
	-it --entrypoint mesos-slave \
	mesosphere/mesos:0.28.1 --ip=127.0.0.1 --hostname=127.0.0.1 

.PHONY: test-environment test-environment-down mesos-master mesos-slave zookeeper

# 'help' parses the Makefile and displays the help text
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

PHONY: help
