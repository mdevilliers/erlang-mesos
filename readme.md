erlang-mesos
------------

An erlang binding for mesos (http://mesos.apache.org/) 

Getting started
---------------

git clone .....
cd erlang-mesos

Dependancies
------------

./rebar compile

How does it work
----------------

Describe the various parts, callbacks


Example framework
-----------------

Example executor
----------------

Help
-----

To debug mesos
GLOG_v=1 erl -pa ebin

Best Practice
-------------

New library therefor run in another node

Todo
----
1. demonstrate nice shutdown, upgrade, break loop
2. example framework, example executor, tutorial
3. upgrade to mesos 0.16
4. auto build the protobuffer files with gdb, clean up depenadancies
5. c/c++ code review
6. resiliance against crashes
7. diazler