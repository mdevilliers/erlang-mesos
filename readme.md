erlang-mesos
------------

An erlang binding for [mesos](http://mesos.apache.org/).

Getting started
---------------

sudo apt-get install g++ (TODO : complete the list)

```
git clone .....
cd erlang-mesos
```

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

erlang-mesos had been implemnted as a [nif](http://www.erlang.org/doc/tutorial/nif.html).
Messages are sent via the nif to mesos and mesos callbacks into erlang asynchronously. Although nifs are never ideal
the asynchonous implmentaion of mesos lends itself nicely in this case.

The modules - scheduler.erl and executor.erl are directly equivilant to their C++ counterparts in mesos.
To implement your own mesos scheduler or framework you implement the behaviours that they expose.

There is an example framework (scheduler) and executor in the src directory.

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
2. example framework, example executor, tutorial - DONE
3. upgrade to mesos 0.16
4. auto build the protobuffer files with gdb, clean up depenadancies -DONE
5. c/c++ code review
6. resiliance against crashes
7. diazler - ISH