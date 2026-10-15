# Upgrading to nacos-sync 2.0

English | [简体中文](UPGRADE.zh-CN.md)

This guide covers moving from nacos-sync 0.5.0 to 2.0 and, after that, from Nacos 2.x to Nacos 3.x. Every measured
number below comes from the `07-upgrade` E2E scenario (see [e2e/README.md](e2e/README.md)): 0.5.0 and 2.0 on one MySQL
database, against Nacos 2.5.4, with tasks of every kind (Eureka, Consul and ZooKeeper in both directions, and Nacos to
Nacos for one service, persistent instances and `ALL`).

## Compatibility

| nacos-sync | JDK | nacos-client | Nacos server | Tested with |
|---|---|---|---|---|
| 2.0 | 17+ | 3.2.4 | 2.x and 3.x | Nacos 2.5.4 and 3.2.4 with auth enabled; Eureka servers from Spring Cloud 2021.0 to 2025.0 (eureka-core 1.10.18 to 2.0.6); Consul 1.20; ZooKeeper 3.7.2; MySQL 8.4 |
| 0.5.0 | 11+ | 1.4.7 | 1.x and 2.x, **not 3.x** | Nacos 2.5.4. Against Nacos 3.2.4 it can neither read nor write: Nacos 3.2 no longer serves the v1 HTTP API it uses (`/nacos/v1/ns/instance` returns 404) |

Nacos to Nacos also works across major versions, so a Nacos 2.x cluster can be copied to a new Nacos 3.x cluster: the
E2E scenarios `04-nacos-nacos`, `05-batch` and `06-interrupt` pass with the source on Nacos 2.5.4 and the target on
3.2.4, and the other way round (51 of 51 checks each way).

For a Nacos 3.x cluster, register the server port (`8848`, not the console port `8080`) and let nacos-sync reach the gRPC
port (`9848`, the server port plus 1000). With auth enabled, set the username and password on the cluster: 2.0 logs in
through `/v3/auth/user/login` and falls back to the v1 login on Nacos 2.x.

## Before you upgrade

1. Upgrade nacos-sync first, and only then move Nacos servers to 3.x: 0.5.0 cannot sync with Nacos 3.x at all.
2. Install JDK 17.
3. Back up the `nacos_sync` database. No migration is needed: 2.0 starts on the 0.5.0 tables and leaves them unchanged.
   Tables created by 0.5.0 use the MyISAM engine and stay that way; converting them to InnoDB is up to you (not tested).
4. If you set `spring.jpa.properties.hibernate.dialect` to `org.hibernate.dialect.MySQL5Dialect`, change it to
   `org.hibernate.dialect.MySQLDialect` or remove it. Hibernate 6, which 2.0 uses, no longer has `MySQL5Dialect`.
5. Read [what behaves differently](#what-behaves-differently-in-20).

## Switching: two ways

Both ways keep every synced instance available: no copy went missing in either of them. They differ in how long the same
endpoint is listed twice and in what happens to task changes. Pick the one whose trade-off suits you.

|  | A. Stop 0.5.0, then start 2.0 | B. Start 2.0, then stop 0.5.0 |
|---|---|---|
| Synced instances missing | Never (0 s measured) | Never (0 s measured) |
| Same endpoint listed twice in Nacos | Up to about 30 s after 0.5.0 stops | For the whole overlap, and about 30 s after 0.5.0 stops |
| nacos-sync console and API | Unavailable while 2.0 starts (about 6 s) | Always available |
| Task changes (add, delete, pause, resume) | Behave as usual | Not shared between the two versions during the overlap |

### A. Stop 0.5.0, then start 2.0

Stop every 0.5.0 node, then start 2.0 on the same database right away. There is no need to wait in between.

- 2.0 registers all copies on its first sync, so nothing goes missing even though it starts while the copies 0.5.0
  registered are still listed.
- Those 0.5.0 copies are removed by Nacos about 30 s after 0.5.0 stops sending heartbeats. Until then, an endpoint synced
  to Nacos can be listed twice: same ip:port, two instance IDs.
- Changes in a source during the few seconds 2.0 needs to start are synced as soon as it is up.

### B. Start 2.0, then stop 0.5.0

Start 2.0 next to the running 0.5.0 on the same database, then stop 0.5.0.

- nacos-sync does not split tasks between nodes: while both run, every task runs on both of them. Every endpoint synced
  to Nacos is listed twice, with different instance IDs, until about 30 s after 0.5.0 stops. Clients that balance load
  per instance send twice its share of traffic to such an endpoint during that time.
- The two versions do not share task changes. A task deleted on 2.0 keeps running on 0.5.0 until 0.5.0 stops; once it
  has stopped, the copies of the deleted task are removed. Avoid changing tasks during the overlap, and keep the overlap
  short.

## Rolling back

Stop 2.0 and start 0.5.0 on the same database. Copies in Nacos destinations are missing for about 5 s: 2.0's copies
disappear as soon as it stops, and 0.5.0 registers them again once it is up. 0.5.0 also runs the tasks created on 2.0.

## Running several nodes

Every nacos-sync node runs every task; the worker IP stored with a task is not used to split them. Each 2.0 node
registers its own copies, so a node going down leaves no gap. A task deleted through one node, however, keeps running on
the other nodes until they restart, because each node only looks at the tasks that still exist. Unless you need that
redundancy, run one active node.

## What behaves differently in 2.0

- Eureka to Nacos syncs only instances whose status is UP. DOWN, OUT_OF_SERVICE and STARTING instances are removed from
  Nacos.
- Nacos to Eureka skips unhealthy and disabled instances and removes copies that become unhealthy or disabled, instead
  of registering them as UP.
- Nacos to Consul deregisters only the instances that went offline; healthy instances are no longer removed and
  registered again.
- Deleting a task (single, batch or an `ALL` task) removes the copies it synced from the destination, also when the
  delete races with a running sync. Instances registered natively in the destination are left alone.
- Nacos to Nacos propagates metadata and `enabled` changes, syncs persistent instances, and keeps the copies registered
  through every sync.
- After a Eureka restart, nacos-sync registers its copies again on the next heartbeat instead of after the 300 s full
  sync.
- Deleting a task that does not exist returns `success: false` with the task ID instead of failing with an error.
- ZooKeeper to Nacos: a task without a group or version registers the providers of each group and version in their own
  Nacos service, and deleting the task removes all of them. A `*` task works with Dubbo 3, which keeps `config`,
  `mapping` and `metadata` under `/dubbo`. A task created before its providers picks them up when they register, and
  a task whose first sync fails, e.g. while the destination is unreachable, starts over on the next round.
- An `ALL` task without a group syncs the services of `DEFAULT_GROUP`, as a task for one service does.
- An `ALL` task keeps its copies when the services of its source cannot be listed, e.g. while the source is
  unreachable, instead of taking every service as gone.
- Adding a task named `ALL` (in any case) is rejected unless the source cluster is Nacos: only a Nacos source can list
  its services, and such a task used to be added and then never synced. To sync every Dubbo service from ZooKeeper,
  use `*`. Tasks of that kind already in the database still do nothing, and nacos-sync logs a warning once for
  each; delete them.
