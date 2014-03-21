erlang-mesos
------------

An erlang binding for [mesos](http://mesos.apache.org/).
Currently working towards support for the 0.18.0 version.

How does it work
----------------

erlang-mesos had been implemented as a [nif](http://www.erlang.org/doc/tutorial/nif.html).
Messages are sent via the nif to mesos and mesos callbacks into erlang asynchronously. Although nifs are never ideal
the asynchonous implmentaion of mesos lends itself nicely in this case.

The modules - scheduler.erl and executor.erl are directly equivialant to their counterparts in mesos.
To add your own mesos scheduler or framework you implement the behaviours that they expose.

There is an example framework (scheduler) and executor in the src directory.

There is also an example of using erlang-mesos in an OTP application at [merkxx](https://github.com/mdevilliers/merkxx).

Getting started
---------------

[Install mesos](http://mesos.apache.org/gettingstarted/) 

Install protobuf version 2.5 - mesos 0.18.0 requires a release >= 2.5

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

To run the example framework from a command window

```
erl -pa ebin 
example_framework:init().
```

Note this will only work on a single node (development) cluster due to the way the example executor is run.
This is only for convenience whilst developing.

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

1. c/c++ code review
2. investigate using a port rather than a nif


Thanks
------

[mesos-go](https://github.com/mesosphere/mesos-go) was an extremely helpful examplar
