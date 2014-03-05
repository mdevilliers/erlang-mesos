erlang-mesos
------------

An erlang binding for mesos (http://mesos.apache.org/) 

Getting started
---------------

git clone .....
cd erlang-mesos

Get going
---------

sudo apt-get install g++ (TODO : complete the list)

Get the project dependancies

```
./rebar get-deps
```

Compile the application including the generated modules from the .proto file

```
./rebar compile
```

After that you should be all set with a 

```
./rebar compile -skip_deps=true
```


How does it work
----------------

Describe the various parts, callbacks


Example framework
-----------------

Example executor
----------------

Help
-----

To debug mesos you can use the following enviromental variable

```
GLOG_v=1 erl -pa ebin
```

Best Practice
-------------

New library therefor run in another node

Todo
----
1. demonstrate nice shutdown, upgrade, break loop - DONE
2. example framework, example executor, tutorial - ISH
3. upgrade to mesos 0.16
4. auto build the protobuffer files with gdb, clean up depenadancies -DONE
5. c/c++ code review
6. resiliance against crashes
7. diazler