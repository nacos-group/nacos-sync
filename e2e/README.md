# nacos-sync E2E

English | [简体中文](README.zh-CN.md)

Runs real applications against a real nacos-sync and real registries, and checks sync behaviour end to end. It complements
the Testcontainers ITs in `nacossync-test`: those cover the sync logic with embedded clients, this suite covers Spring Cloud
and Dubbo applications, real calls through the synced registry, interruptions, batch sizes, Eureka across Spring Cloud
trains, and upgrading from the previous release.

The example applications come from [nacos-sync-example](https://github.com/paderlol/nacos-sync-example/tree/2.0). `run.sh` builds
the committed state of a fixed commit of it (`EXAMPLES_REF` in `run.sh`), the same way it builds nacos-sync.

## What runs where

| Component | Where | Host port |
|---|---|---|
| nacos-sync (built from this checkout) | container | 8083 |
| MySQL 8.4 (nacos-sync database) | container | — |
| Nacos A / Nacos B (`NACOS_VERSION`, or `NACOS_A_VERSION` / `NACOS_B_VERSION` for different tags; auth enabled) | container | 8848+9848 / 18848+19848 (console 28080 / 28081) |
| Eureka Server (built from the examples' `cloud-eureka/eureka-server` for `SC_TRAIN`) | container | 18033 |
| Consul 1.20 (dev agent) | container | 8900 |
| Zookeeper 3.7.2 | container | 2181 |
| Example applications from nacos-sync-example | host (JDK 17) | see each example |
| Only for `07-upgrade`: previous nacos-sync release (`OLD_REF`, same MySQL database) / Nacos v3.2.4 | container | 8084 / 28848 |

nacos-sync runs in a container so that `docker network disconnect` produces a real network partition.

All fixed host ports stay below 32768, so they never clash with Docker's random port mappings. Integration tests that run at the same time must not use them; the nacos-sync Testcontainers ITs keep their fixed Nacos port pairs in 40000-41999.

## Scenarios

| Script | Covers |
|---|---|
| `01-eureka-nacos` | Eureka→Nacos and Nacos→Eureka, bidirectional loop prevention, metadata updates, OUT_OF_SERVICE / disabled instances, graceful stop, kill -9 (lease eviction), real calls through the synced registry, task delete cleanup |
| `02-consul-nacos` | Consul↔Nacos both ways, loop prevention, one instance leaving while its peer stays, offline handling, calls |
| `03-zk-nacos` | Dubbo over Zookeeper↔Nacos both ways, loop prevention, offline handling, Dubbo calls |
| `04-nacos-nacos` | Nacos A↔B, loop prevention, metadata, offline, `serviceName=ALL`, pause/resume |
| `05-batch` | 50×10 Nacos instances via one ALL task, 20 Nacos services to Eureka, 20×5 Eureka instances to Nacos, mass offline, `deleteInBatch` |
| `06-interrupt` | nacos-sync restart with changes while it is down, Nacos down/up, Eureka down/up, task delete/recreate and pause/resume, network partition |
| `07-upgrade` | Upgrade from the previous release (default 0.5.0) on one database: single-node swap, rollback, both versions running at once, cleanup of copies the old release registered, the old release against Nacos 3.2.4. Measures how long synced copies are missing. Needs `-n v2.5.4`; writes `out/report-upgrade-*.md` |

## Running

Prerequisites: Docker, JDK 17 (`JAVA_HOME`), Maven, `curl`, `jq`, `python3`, bash 4+.

```bash
# all scenarios against Nacos 3.2.4 and Eureka from Spring Cloud 2024.0, run from e2e/
JAVA_HOME=/path/to/jdk17 ./run.sh -n v3.2.4 -s 2024.0

# another branch of this checkout, examples from a local nacos-sync-example checkout
./run.sh -b sync-2.x -e ../../nacos-sync-example -n v3.2.4

# Nacos A on 2.5.4 and Nacos B on 3.2.4: Nacos -> Nacos across versions, as when migrating to Nacos 3.x
NACOS_B_VERSION=v3.2.4 ./run.sh -n v2.5.4 04-nacos-nacos 05-batch

# one scenario, keep the environment up afterwards for debugging
./run.sh -n v2.5.4 -k 01-eureka-nacos

# Eureka compatibility across Spring Cloud trains 2021.0 - 2025.0
./matrix.sh -n v3.2.4

# upgrade from 0.5.0 (built with JDK 11 from the tag in this checkout) and back, about 15 minutes
JAVA_HOME=/path/to/jdk17 OLD_JAVA_HOME=/path/to/jdk11 ./run.sh -n v2.5.4 07-upgrade
```

`07-upgrade` options (environment): `OLD_REF` (default `0.5.0`), `OLD_JAVA_HOME` (default `JAVA_HOME`), `OLD_JRE` (runtime image tag, default `11`), `OVERLAP` (seconds both versions run together, default 180), `SWAP_WAIT` (seconds between stopping the old node and starting the new one in the swap case, default 0), `GAP_MAX` (longest acceptable gap for a synced copy, default 60 s). It records a skip on Nacos 3.x, because 0.5.0 only speaks the Nacos v1 HTTP API.

`run.sh` builds the committed state of this checkout (`git archive`, default `HEAD`, change with `-b`, or another checkout with `-r`), so local uncommitted changes never end up in the image. The examples are taken from a bare clone of the GitHub repository cached in `out/cache/`, or from a local checkout given with `-e`; `-E` builds another commit of them. Each run writes its results to `out/runs/<run>.tsv` and a Markdown report to `out/report-*.md`; logs go to `out/logs/`.

Running the ITs and this suite at the same time saturates a laptop's Docker VM (several Nacos JVMs, the batch and upgrade scenarios), which slows everything down and widens timing windows. Run them one after the other.

Credentials in `.env.example` are test-only values for this local environment.
