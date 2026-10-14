# nacos-sync E2E 测试

[English](README.md) | 简体中文

用真实的应用、真实的 nacos-sync 和真实的注册中心，端到端地检查同步行为。它和 `nacossync-test` 里的 Testcontainers
IT 互为补充：IT 用内嵌客户端覆盖同步逻辑，这套测试覆盖 Spring Cloud 和 Dubbo 应用、通过同步后的注册中心发起的真实调用、
各种中断、批量规模、Spring Cloud 各版本系列下的 Eureka，以及从上一个发布版升级。

示例应用来自 [nacos-sync-example](https://github.com/paderlol/nacos-sync-example/tree/2.0)。`run.sh` 会构建它某个固定 commit
（`run.sh` 里的 `EXAMPLES_REF`）上已提交的代码，方式和构建 nacos-sync 一样。

## 各组件运行在哪里

| 组件 | 运行位置 | 宿主机端口 |
|---|---|---|
| nacos-sync（用当前仓库构建） | 容器 | 8083 |
| MySQL 8.4（nacos-sync 的数据库） | 容器 | — |
| Nacos A / Nacos B（`NACOS_VERSION`，两边版本不同时用 `NACOS_A_VERSION` / `NACOS_B_VERSION`；开启鉴权） | 容器 | 8848+9848 / 18848+19848（控制台 28080 / 28081） |
| Eureka Server（用示例仓库的 `cloud-eureka/eureka-server` 按 `SC_TRAIN` 构建） | 容器 | 18033 |
| Consul 1.20（dev agent） | 容器 | 8900 |
| Zookeeper 3.7.2 | 容器 | 2181 |
| nacos-sync-example 里的示例应用 | 宿主机（JDK 17） | 见各示例 |
| 仅 `07-upgrade` 使用：上一个发布版 nacos-sync（`OLD_REF`，共用同一个 MySQL 库）/ Nacos v3.2.4 | 容器 | 8084 / 28848 |

nacos-sync 运行在容器里，这样 `docker network disconnect` 才能制造真实的网络分区。

所有固定的宿主机端口都低于 32768，不会和 Docker 随机分配的端口冲突。同时运行的集成测试不能占用这些端口；nacos-sync 的
Testcontainers IT 把固定的 Nacos 端口对限制在 40000-41999。

## 场景

| 脚本 | 覆盖内容 |
|---|---|
| `01-eureka-nacos` | Eureka→Nacos 和 Nacos→Eureka、双向防回环、元数据更新、OUT_OF_SERVICE 与禁用实例、优雅停机、kill -9（租约剔除）、通过同步后的注册中心发起真实调用、删除任务后清理副本 |
| `02-consul-nacos` | Consul↔Nacos 双向同步、防回环、一个实例下线时保留同伴实例、下线处理、真实调用 |
| `03-zk-nacos` | 基于 Zookeeper 的 Dubbo↔Nacos 双向同步、防回环、下线处理、Dubbo 调用 |
| `04-nacos-nacos` | Nacos A↔B、防回环、元数据、下线、`serviceName=ALL`、暂停与恢复 |
| `05-batch` | 一个 ALL 任务同步 50×10 个 Nacos 实例、20 个 Nacos 服务同步到 Eureka、20×5 个 Eureka 实例同步到 Nacos、批量下线、`deleteInBatch` |
| `06-interrupt` | nacos-sync 停机期间有变化后重启、Nacos 停机与恢复、Eureka 停机与恢复、任务删除后重建、暂停与恢复、网络分区 |
| `07-upgrade` | 在同一个数据库上从上一个发布版（默认 0.5.0）升级：单节点替换、回滚、两个版本同时运行、清理旧版本注册的副本、旧版本对接 Nacos 3.2.4。会测量同步副本缺失的时长。需要 `-n v2.5.4`；结果写入 `out/report-upgrade-*.md` |

## 运行

前置条件：Docker、JDK 17（`JAVA_HOME`）、Maven、`curl`、`jq`、`python3`、bash 4+。

```bash
# 在 e2e/ 目录下，对 Nacos 3.2.4 和 Spring Cloud 2024.0 的 Eureka 跑全部场景
JAVA_HOME=/path/to/jdk17 ./run.sh -n v3.2.4 -s 2024.0

# 测当前仓库的另一个分支，示例应用用本地的 nacos-sync-example
./run.sh -b sync-2.x -e ../../nacos-sync-example -n v3.2.4

# Nacos A 用 2.5.4、Nacos B 用 3.2.4：跨版本的 Nacos -> Nacos 同步，即迁移到 Nacos 3.x 的场景
NACOS_B_VERSION=v3.2.4 ./run.sh -n v2.5.4 04-nacos-nacos 05-batch

# 只跑一个场景，跑完保留环境以便排查
./run.sh -n v2.5.4 -k 01-eureka-nacos

# Spring Cloud 2021.0 - 2025.0 各版本系列下的 Eureka 兼容性
./matrix.sh -n v3.2.4

# 从 0.5.0 升级再回滚（0.5.0 用 JDK 11 从当前仓库的 tag 构建），约 15 分钟
JAVA_HOME=/path/to/jdk17 OLD_JAVA_HOME=/path/to/jdk11 ./run.sh -n v2.5.4 07-upgrade
```

`07-upgrade` 的选项（环境变量）：`OLD_REF`（默认 `0.5.0`）、`OLD_JAVA_HOME`（默认取 `JAVA_HOME`）、`OLD_JRE`（运行时
镜像 tag，默认 `11`）、`OVERLAP`（两个版本同时运行的秒数，默认 180）、`SWAP_WAIT`（单节点替换时停掉旧节点到启动新节点
之间等待的秒数，默认 0）、`GAP_MAX`（同步副本可以接受的最长缺失时长，默认 60 s）。在 Nacos 3.x 上它会记录为跳过，因为
0.5.0 只支持 Nacos v1 HTTP API。

`run.sh` 构建的是当前仓库已提交的代码（`git archive`，默认 `HEAD`，用 `-b` 指定其他 ref，用 `-r` 指定其他仓库），所以
本地未提交的改动永远不会进入镜像。示例应用来自缓存在 `out/cache/` 里的 GitHub 仓库裸克隆，也可以用 `-e` 指定本地仓库；
用 `-E` 可以构建示例仓库的其他 commit。每次运行的结果写入 `out/runs/<run>.tsv`，Markdown 报告写入 `out/report-*.md`，
日志在 `out/logs/` 下。

IT 和这套测试同时运行，会把笔记本上的 Docker 虚拟机占满（多个 Nacos JVM、批量场景和升级场景），所有测试都会变慢，
时序相关的窗口也会被拉长。请一个跑完再跑另一个。

`.env.example` 里的账号密码都是这个本地环境专用的测试值。
