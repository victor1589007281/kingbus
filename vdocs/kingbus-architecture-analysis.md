# Kingbus 架构分析与技术文档

## 项目概述

Kingbus是一个基于Raft一致性算法的分布式MySQL binlog存储系统。它可以作为MySQL主服务器的从库，同时也可以作为其他从库的主服务器，充当中间层的MySQL binlog服务器角色。

### 核心特性

- **MySQL复制协议兼容**：支持GTID模式的binlog同步
- **地理复制支持**：基于Raft算法实现多地域复制
- **高可用性**：确保MySQL binlog复制服务持续可用
- **水平扩展**：支持从库的水平扩展，减轻主库压力
- **异构复制**：支持与Canal等异构复制组件集成

## 系统架构

### 整体架构设计

Kingbus采用分层架构设计，主要包含以下核心层次：

1. **API层**：提供HTTP REST API接口
2. **服务层**：包含Binlog同步器和Binlog服务器
3. **一致性层**：基于Raft算法的分布式一致性
4. **存储层**：分段式磁盘存储系统
5. **网络层**：MySQL协议和Raft通信

### 核心模块架构图

```mermaid
graph TB
    subgraph "Kingbus 核心模块架构"
        subgraph "应用层"
            API[Admin API Server<br/>HTTP REST接口]
            METRICS[Prometheus Server<br/>监控指标收集]
        end
        
        subgraph "服务层"
            SYNCER[Binlog Syncer<br/>MySQL从库模拟器]
            BINLOG_SVR[Binlog Server<br/>MySQL主库模拟器]
            PROGRESS[Binlog Progress<br/>进度跟踪管理]
        end
        
        subgraph "一致性层"
            RAFT_NODE[Raft Node<br/>一致性算法核心]
            CLUSTER[Raft Cluster<br/>集群状态管理]
            MEMBERSHIP[Membership<br/>成员变更管理]
            TRANSPORT[Raft Transport<br/>节点间通信]
        end
        
        subgraph "存储层"
            DISK_STORAGE[Disk Storage<br/>分段式文件存储]
            META_STORAGE[Meta Storage<br/>元数据存储BoltDB]
            SEGMENT[Segment Manager<br/>段文件管理]
            INDEX[Index Manager<br/>索引管理]
        end
        
        subgraph "网络层"
            MYSQL_PROTO[MySQL Protocol<br/>复制协议实现]
            RAFT_HTTP[Raft HTTP<br/>集群通信协议]
            TCP_CONN[TCP Connection<br/>连接管理]
        end
        
        subgraph "外部系统"
            MYSQL_MASTER[MySQL Master<br/>真实主库]
            MYSQL_SLAVES[MySQL Slaves<br/>下游从库]
            CANAL[Canal/其他组件<br/>异构复制]
        end
    end
    
    API --> SYNCER
    API --> BINLOG_SVR
    API --> CLUSTER
    
    SYNCER --> RAFT_NODE
    SYNCER --> MYSQL_PROTO
    BINLOG_SVR --> DISK_STORAGE
    BINLOG_SVR --> MYSQL_PROTO
    PROGRESS --> META_STORAGE
    
    RAFT_NODE --> TRANSPORT
    RAFT_NODE --> DISK_STORAGE
    CLUSTER --> MEMBERSHIP
    TRANSPORT --> RAFT_HTTP
    
    DISK_STORAGE --> SEGMENT
    DISK_STORAGE --> INDEX
    META_STORAGE --> INDEX
    
    MYSQL_PROTO --> TCP_CONN
    RAFT_HTTP --> TCP_CONN
    
    MYSQL_MASTER -.->|"Binlog Stream"| SYNCER
    BINLOG_SVR -.->|"Replication"| MYSQL_SLAVES
    BINLOG_SVR -.->|"Data Feed"| CANAL
    
    METRICS -.->|"监控数据"| API
```

### 核心组件

#### 1. KingbusServer (核心服务器)

```go
type KingbusServer struct {
    Cfg            *config.KingbusServerConfig
    adminSvr       *api.AdminServer      // API服务器
    syncer         *Syncer               // Binlog同步器
    master         *BinlogServer         // Binlog服务器
    binlogProgress *BinlogProgress       // Binlog进度管理
    prometheusSvr  *PrometheusServer     // 监控服务器
    
    id       types.ID
    raftNode *raft.Node                 // Raft节点
    cluster  *membership.RaftCluster    // 集群管理
    store    storage.Storage            // 存储引擎
}
```

**职责**：
- 统一管理所有子服务组件
- 协调Raft一致性和binlog处理
- 处理集群成员变更和故障转移

#### 2. Syncer (Binlog同步器)

```go
type Syncer struct {
    started *atomic.Bool
    cfg     *config.SyncerConfig
    
    conn     *client.Conn
    io       *replication.BinlogSyncer
    ctx      context.Context
    cancel   context.CancelFunc
    
    binlogEventC chan *storagepb.BinlogEvent
    dataC        chan []byte
    store        storage.Storage
}
```

**功能**：
- 作为MySQL从库连接到主库
- 接收并解析MySQL binlog事件
- 将binlog事件提议到Raft集群

#### 3. BinlogServer (Binlog服务器)

```go
type BinlogServer struct {
    started *atomic.Bool
    cfg     *config.BinlogServerConfig
    
    listener net.Listener
    slaves   map[string]*mysql.Slave
    
    broadcast   *utils.Broadcast
    kingbusInfo KingbusInfo
    store       storage.Storage
}
```

**功能**：
- 模拟MySQL主库行为
- 向下游从库发送binlog事件
- 处理MySQL复制协议请求

#### 4. Raft Node (一致性节点)

```go
type Node struct {
    tickMu       *sync.Mutex
    NodeConfig
    
    msgSnapC     chan raftpb.Message
    applyc       chan ApplyEntry
    appliedIndex uint64
    
    committedIndex uint64
    term           uint64
    
    Transport    rafthttp.Transporter
    Storage      storage.Storage
}
```

**功能**：
- 实现Raft一致性算法
- 处理日志复制和领导者选举
- 确保集群数据一致性

#### 5. DiskStorage (存储引擎)

```go
type DiskStorage struct {
    Dir      string
    Segments []*Segment
    
    ReserveSegmentCount int
    purgeStarted        *atomic.Bool
    
    MetaStorage
}
```

**功能**：
- 分段式文件存储
- Raft日志持久化
- 元数据管理和GTID跟踪

## 核心流程分析

### 1. 启动流程

```mermaid
graph TD
    A[main.go启动] --> B[加载配置文件]
    B --> C[初始化日志系统]
    C --> D[创建KingbusServer]
    D --> E[启动Raft节点]
    E --> F[启动Raft通信]
    F --> G[启动Admin API服务]
    G --> H[启动Prometheus监控]
    H --> I[进入主循环]
    I --> J[处理信号和错误]
```

### 2. Binlog同步流程

```mermaid
sequenceDiagram
    participant M as MySQL Master
    participant S as Syncer
    participant R as Raft Cluster
    participant D as DiskStorage
    
    S->>M: 连接并注册为从库
    M->>S: 发送binlog事件
    S->>S: 解析binlog事件
    S->>R: 提议binlog事件
    R->>R: Raft一致性处理
    R->>D: 持久化已提交事件
    D->>D: 更新GTID进度
```

### 3. Binlog分发流程

```mermaid
sequenceDiagram
    participant BS as BinlogServer
    participant SL as MySQL Slave
    participant D as DiskStorage
    participant R as Raft Cluster
    
    SL->>BS: 连接并请求binlog
    BS->>D: 查询binlog事件
    D->>BS: 返回事件数据
    BS->>SL: 发送binlog事件
    R->>BS: 广播新事件通知
    BS->>SL: 实时推送新事件
```

### 4. Raft一致性流程

```mermaid
graph LR
    A[接收提议] --> B{是否为Leader}
    B -->|是| C[追加到本地日志]
    B -->|否| D[转发给Leader]
    C --> E[复制到Follower]
    E --> F[等待多数确认]
    F --> G[提交并应用]
    G --> H[通知客户端]
    D --> I[等待Leader处理]
```

## 数据流架构

### 存储结构设计

#### 1. 分段存储 (Segment Storage)

- **文件分段**：每个段文件默认1GB大小
- **索引机制**：支持快速定位Raft索引
- **压缩清理**：自动清理过期数据

#### 2. 元数据存储 (MetaStorage)

使用BoltDB存储关键元数据：
- Raft状态 (HardState, ConfState)
- GTID集合 (ExecutedGTIDSet, GtidPurged)
- 应用索引 (AppliedIndex)
- 主库信息 (MasterInfo)

#### 3. Binlog事件格式

```protobuf
message BinlogEvent {
    optional uint32 type = 1;
    optional FlavorType flavor = 2;
    optional uint32 divided_count = 3;
    optional uint32 divided_seq_num = 4;
    optional bytes data = 5;
}
```

## 集群管理

### 成员管理

```go
type RaftCluster struct {
    lg     *zap.Logger
    
    localID types.ID
    cid     types.ID
    
    members map[types.ID]*Member
    removed map[types.ID]bool
}
```

**功能**：
- 集群成员添加/删除
- 成员状态监控
- 配置变更管理

### 故障处理

1. **Leader选举**：基于Raft算法的自动故障转移
2. **网络分区**：PreVote机制减少不必要的选举
3. **数据恢复**：快照和日志回放机制

## API接口设计

### REST API端点

- `POST /member` - 添加集群成员
- `DELETE /member/{id}` - 删除集群成员
- `PUT /member/{id}` - 更新成员信息
- `GET /members` - 获取成员列表
- `POST /binlog/syncer/start` - 启动同步器
- `POST /binlog/syncer/stop` - 停止同步器
- `POST /binlog/server/start` - 启动binlog服务
- `POST /binlog/server/stop` - 停止binlog服务

### 配置管理

```yaml
# 节点配置
name: 'kingbus_0'
admin-url: http://127.0.0.1:5000
peer-url: http://127.0.0.1:9696

# 集群配置
initial-cluster: kingbus_0=http://127.0.0.1:9696,kingbus_1=http://127.0.0.1:9697
initial-cluster-state: new

# 性能配置
heartbeat-interval: 200ms
election-timeout: 4000ms
data-dir: /home/kingbus/data
reserve-data-size: 20
```

## 性能优化

### 1. 并发处理

- **异步IO**：Raft写入与消息发送并行
- **批量处理**：批量处理binlog事件
- **管道化**：读写分离的管道设计

### 2. 存储优化

- **预写日志**：WAL机制确保数据持久性
- **压缩算法**：减少存储空间占用
- **缓存机制**：热数据内存缓存

### 3. 网络优化

- **连接复用**：TCP连接池管理
- **压缩传输**：网络数据压缩
- **心跳检测**：及时发现网络故障

## 监控与运维

### Prometheus指标

- `kingbus_raft_applied_index` - Raft应用索引
- `kingbus_binlog_events_total` - Binlog事件总数
- `kingbus_syncer_lag_seconds` - 同步延迟
- `kingbus_cluster_members` - 集群成员数量

### 日志系统

使用结构化日志记录：
- 系统启动和关闭事件
- Raft状态变更
- Binlog处理错误
- 网络连接状态

## 部署架构

### 典型部署拓扑

```mermaid
graph TB
    subgraph "MySQL Master"
        M[MySQL Server]
    end
    
    subgraph "Kingbus Cluster"
        K1[Kingbus Node 1<br/>Leader]
        K2[Kingbus Node 2<br/>Follower]
        K3[Kingbus Node 3<br/>Follower]
        
        K1 -.->|Raft| K2
        K2 -.->|Raft| K3
        K3 -.->|Raft| K1
    end
    
    subgraph "MySQL Slaves"
        S1[MySQL Slave 1]
        S2[MySQL Slave 2]
        S3[MySQL Slave 3]
    end
    
    M -->|Binlog Sync| K1
    K1 -->|Binlog Serve| S1
    K2 -->|Binlog Serve| S2
    K3 -->|Binlog Serve| S3
```

### 容器化部署

```yaml
# docker-compose.yml
version: '3.8'
services:
  kingbus-0:
    image: kingbus:latest
    container_name: kingbus-node-0
    hostname: kingbus-0
    ports:
      - "5000:5000"   # Admin API
      - "9696:9696"   # Raft通信
      - "3390:3390"   # Binlog服务
    volumes:
      - ./data/kingbus-0:/data
      - ./config/kingbus-0.yaml:/etc/kingbus.yaml
      - ./logs/kingbus-0:/logs
    environment:
      - KINGBUS_NODE_NAME=kingbus_0
      - KINGBUS_CLUSTER_STATE=new
    networks:
      - kingbus-network
    restart: unless-stopped
    
  kingbus-1:
    image: kingbus:latest
    container_name: kingbus-node-1
    hostname: kingbus-1
    ports:
      - "5001:5000"
      - "9697:9697"
      - "3391:3390"
    volumes:
      - ./data/kingbus-1:/data
      - ./config/kingbus-1.yaml:/etc/kingbus.yaml
      - ./logs/kingbus-1:/logs
    environment:
      - KINGBUS_NODE_NAME=kingbus_1
      - KINGBUS_CLUSTER_STATE=new
    networks:
      - kingbus-network
    restart: unless-stopped
    
  kingbus-2:
    image: kingbus:latest
    container_name: kingbus-node-2
    hostname: kingbus-2
    ports:
      - "5002:5000"
      - "9698:9698"
      - "3392:3390"
    volumes:
      - ./data/kingbus-2:/data
      - ./config/kingbus-2.yaml:/etc/kingbus.yaml
      - ./logs/kingbus-2:/logs
    environment:
      - KINGBUS_NODE_NAME=kingbus_2
      - KINGBUS_CLUSTER_STATE=new
    networks:
      - kingbus-network
    restart: unless-stopped

  prometheus:
    image: prom/prometheus:latest
    container_name: kingbus-prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus-data:/prometheus
    networks:
      - kingbus-network
    restart: unless-stopped

  grafana:
    image: grafana/grafana:latest
    container_name: kingbus-grafana
    ports:
      - "3000:3000"
    volumes:
      - grafana-data:/var/lib/grafana
      - ./monitoring/dashboards:/etc/grafana/provisioning/dashboards
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin123
    networks:
      - kingbus-network
    restart: unless-stopped

networks:
  kingbus-network:
    driver: bridge
    ipam:
      config:
        - subnet: 172.20.0.0/16

volumes:
  prometheus-data:
  grafana-data:
```

### Kubernetes部署

```yaml
# kingbus-statefulset.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kingbus
  namespace: kingbus-system
spec:
  serviceName: kingbus-headless
  replicas: 3
  selector:
    matchLabels:
      app: kingbus
  template:
    metadata:
      labels:
        app: kingbus
    spec:
      containers:
      - name: kingbus
        image: kingbus:v1.0.0
        ports:
        - containerPort: 5000
          name: admin-api
        - containerPort: 9696
          name: raft-peer
        - containerPort: 3390
          name: binlog-server
        env:
        - name: KINGBUS_NODE_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: KINGBUS_CLUSTER_STATE
          value: "new"
        volumeMounts:
        - name: data
          mountPath: /data
        - name: config
          mountPath: /etc/kingbus.yaml
          subPath: kingbus.yaml
        resources:
          requests:
            cpu: 500m
            memory: 1Gi
          limits:
            cpu: 2000m
            memory: 4Gi
        livenessProbe:
          httpGet:
            path: /cluster
            port: 5000
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /cluster
            port: 5000
          initialDelaySeconds: 5
          periodSeconds: 5
      volumes:
      - name: config
        configMap:
          name: kingbus-config
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      storageClassName: fast-ssd
      resources:
        requests:
          storage: 100Gi
```

## 技术优势

### 1. 架构优势

- **微服务设计**：组件解耦，易于维护
- **插件化架构**：支持功能扩展
- **配置驱动**：灵活的配置管理

### 2. 可靠性保证

- **强一致性**：基于Raft算法的数据一致性
- **故障自愈**：自动故障检测和恢复
- **数据持久化**：多层次的数据保护

### 3. 性能特点

- **高吞吐量**：支持大规模binlog处理
- **低延迟**：优化的网络和存储路径
- **水平扩展**：支持集群规模扩展

## 应用场景

### 1. 读写分离架构

- 减轻主库复制压力
- 支持大量只读从库
- 提高整体系统可用性

### 2. 跨地域复制

- 多数据中心部署
- 灾难恢复支持
- 地理分布式架构

### 3. 数据同步中间件

- 异构系统数据同步
- 实时数据处理管道
- 数据仓库ETL支持

## 总结

Kingbus是一个设计精良的分布式MySQL binlog存储系统，通过Raft一致性算法确保数据可靠性，通过分层架构实现系统的可扩展性和可维护性。其核心优势在于：

1. **高可用性**：基于Raft的分布式一致性保证
2. **高性能**：优化的存储和网络处理
3. **易运维**：完善的监控和管理接口
4. **兼容性好**：完全兼容MySQL复制协议

该系统特别适合需要高可用MySQL复制环境的企业级应用场景。 