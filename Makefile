.PHONY: bootstrap test

test:
	rebar test

bootstrap:
	rebar get-deps

.PHONY: bootstrap

test-environment: zookeeper mesos-master mesos-slave

test-environment-down:
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
	-v "$(pwd)/log/mesos:/var/log/mesos" \
	-v "$(pwd)/tmp/mesos:/var/tmp/mesos" \
	mesosphere/mesos-master:0.28.1

mesos-slave:
	docker run -d --net=host --name mesos-slave --privileged \
	-e MESOS_PORT=5051 \
	-e MESOS_MASTER=zk://127.0.0.1:2181/mesos \
	-e MESOS_SWITCH_USER=0 \
	-e MESOS_CONTAINERIZERS=docker,mesos \
	-e MESOS_LOG_DIR=/var/log/mesos \
	-e MESOS_WORK_DIR=/var/tmp/mesos \
	-v "$(pwd)/log/mesos:/var/log/mesos" \
	-v "$(pwd)/tmp/mesos:/var/tmp/mesos" \
	-v /var/run/docker.sock:/var/run/docker.sock \
	-v /cgroup:/cgroup \
	-v /sys:/sys \
	-v "/usr/bin/docker:/usr/local/bin/docker" \
	-v /lib/x86_64-linux-gnu/libsystemd-journal.so.0:/lib/x86_64-linux-gnu/libsystemd-journal.so.0 \
	-v /usr/lib/x86_64-linux-gnu/libapparmor.so.1:/usr/lib/x86_64-linux-gnu/libapparmor.so.1 \
	-v /usr/lib/x86_64-linux-gnu/libltdl.so.7:/usr/lib/x86_64-linux-gnu/libltdl.so.7 \
	mesosphere/mesos-slave:0.28.1

.PHONY: test-environment test-environment-down mesos-master mesos-slave zookeeper
