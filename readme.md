erlang-mesos
------------

An erlang binding for [mesos](http://mesos.apache.org/).

Getting started
---------------

sudo apt-get install g++ gcc 

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
./rebar compile skip_deps=true

```

To run the example framework from a 

```
example_framework:init().

```

Note this will only work on a single node (development) cluster due to the way the example executor is run.
This is only for convenience whilst developing.


How does it work
----------------

erlang-mesos had been implemented as a [nif](http://www.erlang.org/doc/tutorial/nif.html).
Messages are sent via the nif to mesos and mesos callbacks into erlang asynchronously. Although nifs are never ideal
the asynchonous implmentaion of mesos lends itself nicely in this case.

The modules - scheduler.erl and executor.erl are directly equivialant to their counterparts in mesos.
To implement your own mesos scheduler or framework you implement the behaviours that they expose.

There is an example framework (scheduler) and executor in the src directory. 

Best Practice
-------------

erlang-mesos is a new library interfacing over a nif to a rapidly changing library so for the love of all things holy run please in another erlang node....maybe on mesos!

Help
-----

To debug mesos you can use the following enviromental variable before starting up the erlang runtime.

```
GLOG_v=1 erl -pa ebin
```


Todo
----

1. upgrade to mesos 0.16 - IN PROGRESS
2. auto build the protobuffer files with gdb -DONE (ISH)
3. c/c++ code review

Thanks
------

[mesos-go](https://github.com/mesosphere/mesos-go) was an extremely helpful examplar