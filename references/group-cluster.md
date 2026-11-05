# 课题组集群操作档案

核验日期：**2026-11-05**。本档案与 `clusterflow site show group-cluster` 用于减少重复询问和环境发现；不是实时资源状态。不包含成员账号、凭据、私钥、个人项目目录或作业历史。硬件／软件变更后应更新档案，提交前仍查询实时状态。

## 连接与提交习惯

- 入口 `10.0.43.7:22`，登录／控制节点 **node1**，建议别名 `Cluster`。在已核验的组内网络无需 VPN 或跳板机；换网络后先检查可达性，不承诺公网可达。
- 每位成员使用自己的用户名、认证和本机系统凭据库，不复用其他成员或管理员配置。先读取已有连接／项目映射，不重复初始化。
- 新成员可在本机终端执行 `clusterflow init --site group-cluster --user YOUR_USER --auth password`。站点仅预填地址与别名，不跳过用户确认主机信任、隐藏密码输入或只读验证；AI 不代答信任提示。
- 在 node1 提交，使用 **cpu 分区**，计算节点为 node2–6。GPU 作业指定 **node5**，并显式申请 GPU；仅指定 node5 不会预留显卡。
- node1 的计算节点记录曾为 DOWN，但登录和控制服务可用。不要因此判断集群不可达，也不要擅自把 node1 恢复为计算节点。

## 节点配置

内存为 Slurm 配置容量，单位 **MiB**，不同于系统物理总量。均为每核一个线程。

| 节点 | CPU 型号 | 核数／插槽 | Slurm 内存 | GPU |
|---|---|---|---:|---|
| node2–4 | Intel Xeon Platinum 8370C | 64／2 | 每台 240000 MiB | 无 |
| node5 | Intel Xeon Platinum 8573C | 104／2 | 750000 MiB | 4 × RTX 4090 |
| node6 | AMD EPYC 9654 | 192／2 | 2280000 MiB | 无 |

Intel 与 AMD 可以一起调度；应用二进制和 JIT 缓存的指令集兼容需单独保证。不要盲目跨机型复用 `-march=native` 构建产物，选择兼容构建或隔离缓存。GPU 环境可先限定 node5；不要为了登记 GPU 而统一 CPU 拓扑。

## GPU：已登记并核验

- node5 的 GRES 为 `gpu:rtx_4090:4`。每卡驱动报告 **49140 MiB** 显存，约 48 GiB；不要用普通 4090 产品规格覆盖实测值，仍应在硬件变更后重新检查。
- 按整卡分配，Slurm `--mem`／项目 `memory_mb` 指主机内存，不是显存，不能按任意 GB 数切分显存。
- 单卡、并发单卡、双卡、四卡申请与 CUDA 初始化已实测；单卡作业只能访问已分配设备，CPU-only 作业不能访问 GPU。当前空闲卡数仍需实时查询。
- GPU 自动检测的 NUMA 核范围与现有插槽边界不一致，最终显式映射 GPU 0–1 → CPU 0–51、GPU 2–3 → CPU 52–103，保留 CPU 拓扑。这是已部署配置记录，不授权 AI 自动重装配置。
- 驱动观察值 `595.104.02`。`nvidia-smi` 的 CUDA `13.2` 是驱动支持上限；实际 Toolkit 为 **12.8、12.9**，`/usr/local/cuda` 指向 12.8，`nvcc` 未加入默认 PATH。
- **FEniCSx GPU 软件环境尚未配置／验证。** 有 `fenicsx` Conda 环境不代表 PETSc 已启用 CUDA、装配已在 GPU 执行或实际算例一定加速。

项目资源示例（不是自动默认值）：

```json
{"partition":"cpu","nodelist":"node5","nodes":1,"tasks":1,"cpus_per_task":8,"memory_mb":32768,"gpus_per_node":1,"gpu_type":"rtx_4090","time":"00:30:00"}
```

对应 `--nodelist=node5 --cpus-per-task=8 --gres=gpu:rtx_4090:1`。GPU 数量可改为 2 或 4；进程到卡的绑定由程序／启动命令决定，不自动把四卡解释为四个 MPI 进程。完整待填写映射见 [GPU 项目示例](../examples/project.group-gpu.example.json)。

## 内存：限制尚未修改

当前为 `select/cons_tres`、`CR_CPU`，**未启用内存可消耗资源调度**。`--mem` 记录需求并筛选节点容量，不考虑其他作业内存预留，不通过 task/cgroup 按该请求限制内存；`ConstrainRAMSpace=yes` 也不能单独弥补。`AllocMem=0` 不代表无人使用内存。

保留每个算例的明确内存请求，供后续迁移。若用户另行授权启用内存调度，应单独安排维护：24.11.3 不接受普通重载改变 SelectTypeParameters，既有作业分配记录也没有内存项。不要在成员计算时自行热切换、收紧既有作业限制或顺带重启集群。

## 环境与目录

- node5 已观察到共享 Miniconda `/home/soft/miniconda/2025`，base Python `3.13.12`；可见 `com`、`fenics`、`fenicsx`、`moose` 及部分 `.bak` 目录。可见不代表适用于当前项目，仍要核对依赖、MPI／PETSc 和启动方式。
- 模块目录 `/home/modulefiles`，观察到 `abaqus/2022`、`comsol/61`、`matlab/2024a`。
- `/home` 为共享存储，观察到总容量约 15T；node5 本地根文件系统约 462G。空闲空间实时查 `df`；节点本地文件不要当作跨节点共享结果。
- 本地源、远端代码、运行与结果目录，以及检查／启动命令均按成员和项目绑定。缺失时集中询问，不能套用其他成员的路径。目前没有可复用的组通用项目绑定。
- 不修改共享 Conda/base、系统驱动、全局 PATH 或其他成员文件。环境安装、数值变更和管理员维护分别取得任务授权。

## joblist 与资源查询

node1 已部署彩色 `/usr/bin/joblist`，维护源码为 [`src/clusterflow/joblist.py`](../src/clusterflow/joblist.py)。脚本仅依赖 Python 3.6+ 标准库；本机 ClusterFlow 客户端本身仍要求 Python 3.11+。安装技能不会自动覆盖集群系统命令。个人入口可能遮蔽系统版，遇到不同输出先查 `type -a joblist`。

```text
joblist                     # Slurm 分配每次实时查；硬件快照缓存 10 秒
joblist --refresh           # 并行刷新硬件探测，每节点默认最多 3 秒
joblist --slurm-only        # 无 SSH，仍显示 GPU 分配和作业映射
joblist --json              # 无 ANSI 颜色的结构化输出
joblist --color=always      # 显式保留配色
```

分别显示 Slurm 分配卡数、有计算进程的卡数、采样利用率大于零的卡数。已分配但利用率为零不等于没被占用；直接 SSH 运行可能有计算进程而没有 Slurm 分配。缓存硬件数据与调度状态有时间差，核对时刷新，不把多次查询说成原子快照。

每节点只有一行内存概览，说明放顶部一次；无 GPU 节点不显示占位行或作业的 GPU 零值列。交互终端彩色，重定向和 JSON 默认纯文本。

node6 的普通用户 SSH 实时探测曾多次超时，但 Slurm 作业仍运行。超时只说明实时数据未知，不据此判断节点故障、取消作业或改变节点状态；回退时不能把配置容量当作实际可用内存。

## 管理边界

日常查询和算例使用成员账号，明确要求维护时才使用单独的管理员连接。档案不授予 root／sudo 或维护授权。维护 node5 时先阻止新分配，检查 Slurm 和直接 SSH 启动的 CPU/GPU 程序，备份并验证配置，尽量不触及其他节点。不要照抄历史脚本重启整个集群或修改运行中科研任务。

官方参考：[GPU GRES](https://slurm.schedmd.com/gres.html)、[内存资源调度](https://slurm.schedmd.com/cons_tres_share.html)、[24.11.3 配置重读实现](https://github.com/SchedMD/slurm/blob/slurm-24-11-3-1/src/slurmctld/read_config.c#L2258-L2278)。
