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
     

## Task remains (for 虎牙 Project):

 - 数据库，增加字段，增加task_type, 比如 nacos-> nacos, or dubbo_zk -> nacos. 目前仅支持内部ConfigServer->Nacos
 - 代码逻辑抽象，要吧现在的 SyncManagerService 改下，区分开不同的任务类型用不同的同步方式
 - Console, 还未实现，目前用户只能通过API来访问：http://127.0.0.1:8081/skywalker/swagger-ui.html#/
 - 重构代码，以NacosSync替换Skywalker，并去掉阿里内部特有的Driver，如数据库Driver。
