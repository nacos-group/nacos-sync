# NacosSync

## Function

 - Console: provide API and console for management
 - Worker: provider the service registration synchronization. 

## Architecture

### Architecture Topology


```
            +-------------+
     +----> |NacosClusterA|
     |      +-------------+               +-------------+
     |                                    |NacosClusterB|
Pull |                                    +--+----------+
Info |      +------------+                   ^
     |      |ZooKeeper   |                   |
     |      +--+---------+                   | Push Info
     |         ^ Pull Info                   |
     |         |                             |
     |        ++-----------------------------+--+
     <--------+  NacosSync1, NacosSync2,....    |
              +---+-------------------------+---+
                  |                         |
                  |                         |
                  |                         |
                  |       +---------+       |
                  +-----> |NacosSync| <-----+
                          |Database |
                          +---------+
```

### Architecture HighLights

 - All registration information will be stored in NacosSync DB.
 - Multiple NacosSync instances will perform the same job.
     - Multiple NacosSync instances ensure high availability.
     - Multiple NacosSync instances performing the same job ensure the simplicity.
     - NacosCluster target will dedup the synchronization information from Nacos.
     

## Quick Start:
 - Swagger API: http://127.0.0.1:8081/skywalker/swagger-ui.html#/
 - Others: TBD

