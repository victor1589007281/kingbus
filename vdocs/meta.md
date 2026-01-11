# Kingbus 元数据管理分析

## 概述

本文档详细分析 Kingbus 系统中的所有元数据，包括元数据的类型、存储方式、产生来源、维护机制等。

## 1. 元数据总览

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '12px', 'fontFamily': 'Arial', 'primaryColor': '#e3f2fd', 'primaryTextColor': '#000000', 'primaryBorderColor': '#1976d2', 'lineColor': '#1976d2', 'background': '#ffffff', 'clusterBkg': '#f5f5f5', 'clusterBorder': '#999999', 'edgeLabelBackground': '#ffffff'}}}%%
graph TB
    subgraph "Kingbus 元数据体系"
        
        subgraph "Raft元数据"
            HS["HardState<br/>Term/Vote/Commit"]
            CS["ConfState<br/>集群配置"]
            AI["AppliedIndex<br/>已应用索引"]
            RC["RaftCluster<br/>集群信息"]
        end
        
        subgraph "Binlog元数据"
            EG["ExecutedGtidSet<br/>已执行GTID"]
            GP["GtidPurged<br/>已清理GTID"]
            FDE["FDE<br/>格式描述事件"]
            PGE["PreviousGtidSet<br/>前置GTID"]
            NB["NextBinlogFile<br/>下一个binlog文件"]
        end
        
        subgraph "业务元数据"
            SA["SyncerArgs<br/>同步器参数"]
            MI["MasterInfo<br/>主库信息"]
            NR["NeedRecover<br/>恢复标志"]
        end
        
        subgraph "存储位置"
            BOLT["BoltDB<br/>kingbus_meta.db"]
        end
    end
    
    HS --> BOLT
    CS --> BOLT
    AI --> BOLT
    RC --> BOLT
    EG --> BOLT
    GP --> BOLT
    FDE --> BOLT
    PGE --> BOLT
    NB --> BOLT
    SA --> BOLT
    MI --> BOLT
    NR --> BOLT

    style HS fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
    style EG fill:#e3f2fd,stroke:#0d47a1,stroke-width:2px
    style SA fill:#fff3e0,stroke:#e65100,stroke-width:2px
    style BOLT fill:#fce4ec,stroke:#880e4f,stroke-width:2px
```

## 2. 元数据详细说明

### 2.1 元数据清单

| 键名 | 类型 | 功能描述 | 产生模块 | 更新时机 |
|-----|------|---------|---------|---------|
| **hard_state** | Raft | Raft硬状态(Term/Vote/Commit) | Raft | 每次状态变更 |
| **conf_state** | Raft | 集群成员配置 | Raft | 成员变更时 |
| **apply_index** | Raft | 已应用的Raft索引 | Apply | 事务边界 |
| **raft_cluster** | Raft | 集群成员详细信息 | Membership | 成员变更时 |
| **mysql/executed_gtids** | Binlog | 已执行的GTID集合 | BinlogProgress | 事务完成时 |
| **mysql/gtid_purged** | Binlog | 已清理的GTID集合 | PurgeLog | 日志清理时 |
| **fde/{raftIndex}** | Binlog | FORMAT_DESCRIPTION_EVENT | Apply | 新binlog文件 |
| **pre_gtids/{raftIndex}** | Binlog | PREVIOUS_GTIDS_EVENT | Apply | 新binlog文件 |
| **next_binlog/{raftIndex}** | Binlog | 下一个binlog文件名 | Apply | ROTATE事件 |
| **syncer_args** | 业务 | Syncer启动参数 | AdminAPI | 启动Syncer时 |
| **master_info** | 业务 | 上游MySQL主库信息 | Syncer | 启动Syncer时 |
| **ds_need_recover** | 系统 | 存储是否需要恢复 | Storage | 启动/关闭时 |

### 2.2 元数据存储结构

```go
// storage/storage.go 中定义的常量
const (
    HardStateKey       = "hard_state"        // Raft硬状态
    ConfStateKey       = "conf_state"        // 集群配置状态
    ExecutedGtidSetKey = "executed_gtids"    // 已执行GTID
    GtidPurgedKey      = "gtid_purged"       // 已清理GTID
    FdePrefix          = "fde"               // FDE前缀
    NextBinlogPrefix   = "next_binlog"       // 下一binlog前缀
    MasterInfoKey      = "master_info"       // 主库信息
    PgePrefix          = "pre_gtids"         // 前置GTID前缀
    SyncerArgsKey      = "syncer_args"       // Syncer参数
    NeedRecoverKey     = "ds_need_recover"   // 恢复标志
    RaftClusterKey     = "raft_cluster"      // 集群信息
    AppliedIndexKey    = "apply_index"       // 已应用索引
)
```

## 3. 元数据功能分类

### 3.1 Raft一致性元数据

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '12px', 'fontFamily': 'Arial', 'primaryColor': '#e3f2fd', 'primaryTextColor': '#000000', 'primaryBorderColor': '#1976d2', 'lineColor': '#1976d2', 'background': '#ffffff', 'clusterBkg': '#f5f5f5', 'clusterBorder': '#999999', 'edgeLabelBackground': '#ffffff'}}}%%
graph LR
    subgraph "Raft元数据生命周期"
        
        subgraph "HardState"
            HS_T["Term<br/>当前任期"]
            HS_V["Vote<br/>投票对象"]
            HS_C["Commit<br/>提交索引"]
        end
        
        subgraph "产生时机"
            HS_P1["Leader选举"]
            HS_P2["投票请求"]
            HS_P3["日志提交"]
        end
        
        subgraph "存储方式"
            HS_S["JSON序列化<br/>写入BoltDB"]
        end
    end
    
    HS_P1 --> HS_T
    HS_P2 --> HS_V
    HS_P3 --> HS_C
    
    HS_T --> HS_S
    HS_V --> HS_S
    HS_C --> HS_S

    style HS_T fill:#e8f5e9,stroke:#1b5e20
    style HS_V fill:#e3f2fd,stroke:#0d47a1
    style HS_C fill:#fff3e0,stroke:#e65100
    style HS_S fill:#fce4ec,stroke:#880e4f
```

**HardState详解**：

```go
// 存储位置: meta_storage.go
func (s *MetaStore) SaveHardState(st pb.HardState) error {
    value, _ := json.Marshal(st)  // JSON序列化
    return s.Set(utils.StringToBytes(HardStateKey), value)
}

// HardState结构 (来自etcd/raft)
type HardState struct {
    Term   uint64  // 当前任期
    Vote   uint64  // 投票给的节点ID
    Commit uint64  // 已提交的日志索引
}
```

**ConfState详解**：

```go
// 集群配置状态
type ConfState struct {
    Nodes    []uint64  // 当前节点列表
    Learners []uint64  // 学习者节点列表
}

// 存储时机: 成员变更后
func saveConfState(store storage.Storage, confState *raftpb.ConfState) {
    value, _ := json.Marshal(confState)
    store.Set(utils.StringToBytes(storage.ConfStateKey), value)
}
```

### 3.2 Binlog进度元数据

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '12px', 'fontFamily': 'Arial', 'primaryColor': '#e3f2fd', 'primaryTextColor': '#000000', 'primaryBorderColor': '#1976d2', 'lineColor': '#333333', 'signalColor': '#333333', 'signalTextColor': '#1976d2', 'actorBkg': '#e3f2fd', 'actorBorder': '#1976d2', 'actorTextColor': '#000000', 'activationBkgColor': '#e8f5e9', 'noteBkgColor': '#fff3e0', 'noteTextColor': '#000000', 'noteBorderColor': '#f57c00', 'loopTextColor': '#000000', 'labelTextColor': '#1976d2', 'background': '#ffffff'}}}%%
sequenceDiagram
    participant Master as MySQL Master
    participant Syncer as Syncer
    participant Apply as Apply
    participant Meta as MetaStorage
    participant Slave as MySQL Slave
    
    rect rgb(232, 245, 233)
        Note over Master,Meta: 1. ExecutedGtidSet 更新流程
        Master->>Syncer: binlog events
        Syncer->>Apply: Raft提交
        Apply->>Apply: 解析GTID_EVENT
        Apply->>Apply: 检测事务边界
        Apply->>Meta: SetBinlogProgress(index, gtidSet)
    end
    
    rect rgb(227, 242, 253)
        Note over Master,Meta: 2. GtidPurged 更新流程
        Apply->>Apply: 日志清理触发
        Apply->>Meta: UpdatePurgedGtidset(firstIndex)
        Meta->>Meta: 计算已清理GTID
        Meta->>Meta: 删除过期PreviousGtidSet
    end
    
    rect rgb(255, 243, 224)
        Note over Slave,Meta: 3. Slave连接时查询
        Slave->>Meta: GetGtidSet(executed_gtids)
        Slave->>Meta: GetGtidSet(gtid_purged)
        Meta->>Slave: 返回GTID集合
    end
```

**ExecutedGtidSet 管理**：

```go
// storage/meta_storage.go
// 键格式: mysql/executed_gtids
func (s *MetaStore) SetBinlogProgress(appliedIndex uint64, executedGtidSet gomysql.GTIDSet) error {
    // 原子更新: 同时保存appliedIndex和executedGtidSet
    return s.DB.Update(func(tx *bolt.Tx) error {
        b := tx.Bucket(s.BucketName)
        // 保存GTID
        b.Put(gtidsKey, executedGtidSet.Encode())
        // 保存appliedIndex
        b.Put(appliedIndexKey, appliedIndexValue)
        return nil
    })
}
```

**更新触发条件** (binlog_progress.go):

```go
// 每10000个事务 或 每4秒 持久化一次
const (
    persistentCount        = 10000
    persistentTimeInterval = 4 * time.Second
)

func (s *BinlogProgress) updateProcess(raftIndex uint64, eventRawData []byte) error {
    // 检测事务边界
    if s.trxBoundaryParser.IsNotInsideTransaction() {
        // 更新executedGtidSet
        s.executedGtidSet.Update(currentGtidStr)
        
        // 触发持久化检查
        if raftIndex-s.persistentAppliedIndex > persistentCount ||
           time.Since(s.persistentTime) > persistentTimeInterval {
            s.store.SetBinlogProgress(raftIndex, s.executedGtidSet)
        }
    }
}
```

### 3.3 Binlog文件元数据

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '12px', 'fontFamily': 'Arial', 'primaryColor': '#e3f2fd', 'primaryTextColor': '#000000', 'primaryBorderColor': '#1976d2', 'lineColor': '#1976d2', 'background': '#ffffff', 'clusterBkg': '#f5f5f5', 'clusterBorder': '#999999', 'edgeLabelBackground': '#ffffff'}}}%%
graph TB
    subgraph "Binlog文件元数据管理"
        
        subgraph "FDE (Format Description Event)"
            FDE_K["Key: fde/{raftIndex}"]
            FDE_V["Value: 完整FDE二进制"]
            FDE_T["时机: 每个binlog文件开头"]
        end
        
        subgraph "PreviousGtidSet"
            PGE_K["Key: pre_gtids/{raftIndex}"]
            PGE_V["Value: GTID集合编码"]
            PGE_T["时机: 每个binlog文件开头"]
        end
        
        subgraph "NextBinlogFile"
            NB_K["Key: next_binlog/{raftIndex}"]
            NB_V["Value: mysql-bin.000002"]
            NB_T["时机: ROTATE事件"]
        end
    end

    style FDE_K fill:#e8f5e9,stroke:#1b5e20
    style PGE_K fill:#e3f2fd,stroke:#0d47a1
    style NB_K fill:#fff3e0,stroke:#e65100
```

**用途说明**：

| 元数据 | 用途 |
|-------|------|
| **FDE** | Slave连接时发送，包含binlog版本信息 |
| **PreviousGtidSet** | 定位Slave开始复制的位置 |
| **NextBinlogFile** | 构造ROTATE事件，告知Slave当前binlog文件 |

### 3.4 业务配置元数据

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '12px', 'fontFamily': 'Arial', 'primaryColor': '#e3f2fd', 'primaryTextColor': '#000000', 'primaryBorderColor': '#1976d2', 'lineColor': '#1976d2', 'background': '#ffffff', 'clusterBkg': '#f5f5f5', 'clusterBorder': '#999999', 'edgeLabelBackground': '#ffffff'}}}%%
graph TB
    subgraph "业务配置元数据"
        
        subgraph "SyncerArgs"
            SA_K["Key: syncer_args"]
            SA_S["结构体:"]
            SA_F1["• syncer_id: int"]
            SA_F2["• syncer_uuid: string"]
            SA_F3["• mysql_addr: string"]
            SA_F4["• mysql_user: string"]
            SA_F5["• mysql_password: string"]
            SA_F6["• semi_sync: bool"]
            SA_U["用途: Leader切换后自动重启Syncer"]
        end
        
        subgraph "MasterInfo"
            MI_K["Key: master_info"]
            MI_S["结构体:"]
            MI_F1["• server_id: int32"]
            MI_F2["• server_uuid: string"]
            MI_F3["• version: string"]
            MI_U["用途: BinlogServer模拟MySQL主库"]
        end
    end

    style SA_K fill:#e8f5e9,stroke:#1b5e20
    style MI_K fill:#e3f2fd,stroke:#0d47a1
```

**SyncerArgs持久化目的**：

```go
// server/server.go - Leader切换时自动恢复Syncer
func (s *KingbusServer) updateLeadership(leadChange bool) {
    if leadChange && s.IsLeader() {
        go func() {
            // 等待10秒确保旧Leader已停止
            <-time.After(time.Second * 10)
            
            // 从存储读取Syncer参数
            value, _ := s.store.Get(utils.StringToBytes(storage.SyncerArgsKey))
            if value != nil {
                var syncerArgs config.SyncerArgs
                json.Unmarshal(value, &syncerArgs)
                // 自动启动Syncer
                s.StartServer(config.SyncerServerType, &syncerArgs)
            }
        }()
    }
}
```

## 4. 元数据维护机制

### 4.1 写入流程

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '12px', 'fontFamily': 'Arial', 'primaryColor': '#e3f2fd', 'primaryTextColor': '#000000', 'primaryBorderColor': '#1976d2', 'lineColor': '#1976d2', 'background': '#ffffff', 'clusterBkg': '#f5f5f5', 'clusterBorder': '#999999', 'edgeLabelBackground': '#ffffff'}}}%%
flowchart TB
    subgraph "元数据写入流程"
        
        A["事件产生"] --> B{"事件类型"}
        
        B -->|Raft状态变更| C["SaveHardState"]
        B -->|成员变更| D["saveConfState"]
        B -->|GTID事务完成| E["SetBinlogProgress"]
        B -->|新Binlog文件| F["Set FDE/PGE/NextBinlog"]
        B -->|配置变更| G["Set SyncerArgs/MasterInfo"]
        
        C --> H["BoltDB.Update"]
        D --> H
        E --> H
        F --> H
        G --> H
        
        H --> I["写入磁盘"]
    end

    style A fill:#e1f5fe,stroke:#01579b
    style H fill:#fce4ec,stroke:#880e4f,stroke-width:2px
    style I fill:#e8f5e9,stroke:#1b5e20
```

### 4.2 读取流程

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '12px', 'fontFamily': 'Arial', 'primaryColor': '#e3f2fd', 'primaryTextColor': '#000000', 'primaryBorderColor': '#1976d2', 'lineColor': '#1976d2', 'background': '#ffffff', 'clusterBkg': '#f5f5f5', 'clusterBorder': '#999999', 'edgeLabelBackground': '#ffffff'}}}%%
flowchart TB
    subgraph "元数据读取场景"
        
        subgraph "节点启动"
            S1["InitialState()"]
            S2["读取HardState"]
            S3["读取ConfState"]
            S4["读取AppliedIndex"]
            S5["读取ExecutedGtidSet"]
        end
        
        subgraph "Slave连接"
            C1["CheckGtidSet()"]
            C2["读取ExecutedGtidSet"]
            C3["读取GtidPurged"]
            C4["GetPreviousGtidSet()"]
            C5["GetFde()"]
        end
        
        subgraph "Leader切换"
            L1["updateLeadership()"]
            L2["读取SyncerArgs"]
            L3["自动启动Syncer"]
        end
    end

    S1 --> S2
    S2 --> S3
    S3 --> S4
    S4 --> S5
    
    C1 --> C2
    C2 --> C3
    C3 --> C4
    C4 --> C5
    
    L1 --> L2
    L2 --> L3

    style S1 fill:#e8f5e9,stroke:#1b5e20
    style C1 fill:#e3f2fd,stroke:#0d47a1
    style L1 fill:#fff3e0,stroke:#e65100
```

### 4.3 清理机制

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '12px', 'fontFamily': 'Arial', 'primaryColor': '#e3f2fd', 'primaryTextColor': '#000000', 'primaryBorderColor': '#1976d2', 'lineColor': '#333333', 'signalColor': '#333333', 'signalTextColor': '#1976d2', 'actorBkg': '#e3f2fd', 'actorBorder': '#1976d2', 'actorTextColor': '#000000', 'activationBkgColor': '#e8f5e9', 'noteBkgColor': '#fff3e0', 'noteTextColor': '#000000', 'noteBorderColor': '#f57c00', 'loopTextColor': '#000000', 'labelTextColor': '#1976d2', 'background': '#ffffff'}}}%%
sequenceDiagram
    participant PurgeLog as PurgeLog
    participant Storage as DiskStorage
    participant Meta as MetaStorage
    
    rect rgb(255, 243, 224)
        Note over PurgeLog,Meta: 日志清理流程(每30秒检查)
    end
    
    PurgeLog->>Storage: 检查Segment数量
    alt 超过ReserveSegmentCount
        Storage->>Storage: 确定要删除的Segments
        Storage->>Meta: UpdatePurgedGtidset(firstIndex)
        Meta->>Meta: 读取临近的PreviousGtidSet
        Meta->>Meta: 更新GtidPurged
        Meta->>Meta: 删除过期PreviousGtidSet
        Storage->>Storage: 删除Segment文件
    end
```

**清理代码**：

```go
// storage/meta_storage.go
func (s *MetaStore) UpdatePugedGtidset(firstIndex uint64) error {
    // 1. 查找firstIndex之后最近的PreviousGtidSet
    // 2. 将其合并到GtidPurged
    // 3. 删除firstIndex之前的所有PreviousGtidSet
}

// storage/disk_storage.go - 触发时机
func (s *DiskStorage) StartPurgeLog() {
    timer := time.NewTimer(time.Second * 30)
    for {
        select {
        case <-timer.C:
            if s.ReserveSegmentCount < len(s.Segments) {
                // 清理超出保留数量的Segments
                s.purgeSegments(deleteSegments, true)
            }
        }
    }
}
```

## 5. 元数据一致性保证

### 5.1 原子性保证

```go
// SetBinlogProgress 原子更新多个元数据
func (s *MetaStore) SetBinlogProgress(appliedIndex uint64, executedGtidSet gomysql.GTIDSet) error {
    return s.DB.Update(func(tx *bolt.Tx) error {
        b := tx.Bucket(s.BucketName)
        // 在同一事务中更新两个键
        b.Put(gtidsKey, gtidsValue)
        b.Put(appliedIndexKey, appliedIndexValue)
        return nil
    })
}
```

### 5.2 一致性恢复

```go
// 启动时检查是否需要恢复
func (s *DiskStorage) initSegments() {
    needRecover := s.getRecover()  // 读取 ds_need_recover
    s.Segments = openSegments(s.Dir, needRecover)
}

// 正常关闭时清除恢复标志
func (s *DiskStorage) Close() error {
    s.setRecover(false)  // 设置 ds_need_recover = false
    s.MetaStorage.Close2()
}

// 如果异常退出，下次启动会重建索引
func openSegment(dir, name string, status SegmentStatus, needRecover bool) *Segment {
    if needRecover {
        s.LogIndex = s.rebuildIndex()  // 从Segment重建索引
    }
}
```

## 6. 元数据监控与诊断

### 6.1 关键指标

| 指标 | 含义 | 告警阈值 |
|-----|------|---------|
| **applied_index** | 已应用Raft索引 | 与committed_index差距过大 |
| **executed_gtid_set** | 已执行GTID | 与Master差距过大 |
| **gtid_purged** | 已清理GTID | 无法满足Slave需求 |

### 6.2 诊断工具

```bash
# 使用tools/meta_store查看元数据
go run tools/meta_store/meta_store.go -d /path/to/data

# 查看存储的所有键值
# 可以检查:
# - executed_gtids: 当前进度
# - gtid_purged: 已清理范围
# - syncer_args: Syncer配置
# - master_info: 主库信息
```

## 7. 总结

### 7.1 元数据分类汇总

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '12px', 'fontFamily': 'Arial', 'primaryColor': '#e3f2fd', 'primaryTextColor': '#000000', 'primaryBorderColor': '#1976d2', 'lineColor': '#1976d2', 'background': '#ffffff', 'clusterBkg': '#f5f5f5', 'clusterBorder': '#999999', 'edgeLabelBackground': '#ffffff'}}}%%
pie title Kingbus元数据分类
    "Raft元数据" : 4
    "Binlog进度元数据" : 2
    "Binlog文件元数据" : 3
    "业务配置元数据" : 3
```

### 7.2 维护要点

1. **Raft元数据**: 由etcd/raft库自动管理，通过SaveHardState持久化
2. **Binlog进度**: 基于事务边界更新，批量持久化减少IO
3. **Binlog文件**: 在特定事件时记录，用于Slave定位
4. **业务配置**: 通过Raft Propose保证集群一致，支持Leader切换恢复
5. **一致性恢复**: 通过NeedRecover标志和索引重建保证数据完整性

