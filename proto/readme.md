


1. Copy proto files from /usr/include/mesos
2. In scheduler.proto change 

```
import "mesos/v1/mesos.proto";
``` 

to 

```
import "mesos.proto";
```
