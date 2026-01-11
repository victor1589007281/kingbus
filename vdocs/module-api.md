# Kingbus API 模块分析

## 1. 模块概述

**API** 模块是 Kingbus 的管理接口层，基于 Echo Web 框架实现 RESTful API，提供集群管理、Syncer 控制、BinlogServer 控制等功能。

### 核心职责

- 集群成员管理（添加、删除、更新节点）
- Syncer 生命周期管理（启动、停止、状态查询）
- BinlogServer 生命周期管理
- 请求转发（非 Leader 节点转发到 Leader）
- 日志记录和错误恢复

---

## 2. 架构图

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '12px', 'fontFamily': 'Arial', 'primaryColor': '#e3f2fd', 'primaryTextColor': '#000000', 'primaryBorderColor': '#1976d2', 'lineColor': '#1976d2', 'background': '#ffffff', 'clusterBkg': '#f5f5f5', 'clusterBorder': '#999999', 'edgeLabelBackground': '#ffffff'}}}%%
graph TB
    subgraph "客户端"
        CLI["CLI / curl"]
        ADMIN["Admin UI"]
    end
    
    subgraph "API 模块"
        subgraph "AdminServer"
            AS["AdminServer<br/>api/api_server.go:34"]
            ECHO["Echo Web<br/>HTTP 框架"]
            MW["Middleware<br/>Logger + Recover"]
        end
        
        subgraph "Handlers"
            MH["MembershipHandler<br/>集群管理"]
            BSH["BinlogSyncerHandler<br/>Syncer 管理"]
            BMH["BinlogServerHandler<br/>BinlogServer 管理"]
        end
    end
    
    subgraph "KingbusServer"
        KS["Server Interface<br/>api/api_server.go:43"]
        RAFT["Raft Node"]
        CLUSTER["RaftCluster"]
        SYNCER["Syncer"]
        MASTER["BinlogServer"]
    end
    
    CLI --> AS
    ADMIN --> AS
    AS --> ECHO
    ECHO --> MW
    MW --> MH & BSH & BMH
    
    MH --> KS
    BSH --> KS
    BMH --> KS
    
    KS --> RAFT
    KS --> CLUSTER
    KS --> SYNCER
    KS --> MASTER
    
    style AS fill:#e3f2fd,stroke:#1976d2,stroke-width:2px
    style MH fill:#e8f5e9,stroke:#388e3c,stroke-width:2px
    style BSH fill:#fff3e0,stroke:#f57c00,stroke-width:2px
    style BMH fill:#fce4ec,stroke:#c2185b,stroke-width:2px
```

---

## 3. API 路由表

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '12px', 'fontFamily': 'Arial', 'primaryColor': '#e3f2fd', 'primaryTextColor': '#000000', 'primaryBorderColor': '#1976d2', 'lineColor': '#1976d2', 'background': '#ffffff', 'clusterBkg': '#f5f5f5', 'clusterBorder': '#999999', 'edgeLabelBackground': '#ffffff'}}}%%
graph TB
    subgraph "API 路由结构"
        ROOT["AdminServer<br/>:9595"]
        
        subgraph "集群管理 /members"
            M1["GET /members<br/>获取成员列表"]
            M2["POST /members<br/>添加成员"]
            M3["PUT /members<br/>更新成员"]
            M4["DELETE /members<br/>删除成员"]
            M5["GET /cluster<br/>获取集群信息"]
            M6["PUT /admin/url<br/>更新 Admin URL"]
        end
        
        subgraph "Syncer 管理 /binlog/syncer"
            S1["PUT /binlog/syncer/start<br/>启动 Syncer"]
            S2["PUT /binlog/syncer/stop<br/>停止 Syncer"]
            S3["GET /binlog/syncer/status<br/>获取状态"]
        end
        
        subgraph "BinlogServer 管理 /binlog/server"
            B1["PUT /binlog/server/start<br/>启动 BinlogServer"]
            B2["PUT /binlog/server/stop<br/>停止 BinlogServer"]
            B3["GET /binlog/server/status<br/>获取状态"]
        end
        
        ROOT --> M1 & M2 & M3 & M4 & M5 & M6
        ROOT --> S1 & S2 & S3
        ROOT --> B1 & B2 & B3
    end
    
    style ROOT fill:#e3f2fd,stroke:#1976d2,stroke-width:2px
    style M1 fill:#e8f5e9,stroke:#388e3c,stroke-width:2px
    style S1 fill:#fff3e0,stroke:#f57c00,stroke-width:2px
    style B1 fill:#fce4ec,stroke:#c2185b,stroke-width:2px
```

---

## 4. 时序图

### 4.1 启动 Syncer 流程

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '12px', 'fontFamily': 'Arial', 'primaryColor': '#e3f2fd', 'primaryTextColor': '#000000', 'primaryBorderColor': '#1976d2', 'lineColor': '#333333', 'signalColor': '#333333', 'signalTextColor': '#1976d2', 'actorBkg': '#e3f2fd', 'actorBorder': '#1976d2', 'actorTextColor': '#000000', 'activationBkgColor': '#e8f5e9', 'noteBkgColor': '#fff3e0', 'noteTextColor': '#000000', 'noteBorderColor': '#f57c00', 'loopTextColor': '#000000', 'labelTextColor': '#1976d2', 'background': '#ffffff'}}}%%
sequenceDiagram
    autonumber
    participant CLI as Client
    participant AS as AdminServer
    participant BSH as BinlogSyncerHandler
    participant KS as KingbusServer
    participant RAFT as Raft
    
    CLI->>AS: PUT /binlog/syncer/start
    AS->>BSH: StartBinlogSyncer(ctx)
    
    BSH->>BSH: echoCtx.Bind(&args)
    BSH->>BSH: args.Check()
    
    alt 非 Leader 节点
        BSH->>BSH: svr.IsLeader() == false
        BSH->>BSH: sendToLeader("PUT", "/binlog/syncer/start", req)
        Note over BSH: 转发到 Leader
        BSH-->>CLI: Leader 响应
    else Leader 节点
        BSH->>KS: StartServer(SyncerServerType, &args)
        KS->>KS: startSyncerServer(args)
        KS-->>BSH: 成功
        
        BSH->>BSH: ProposeSyncerArgs(&args)
        BSH->>KS: Propose(data)
        KS->>RAFT: 提交到 Raft
        
        BSH-->>CLI: 200 OK
    end
```

### 4.2 添加集群成员流程

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '12px', 'fontFamily': 'Arial', 'primaryColor': '#e3f2fd', 'primaryTextColor': '#000000', 'primaryBorderColor': '#1976d2', 'lineColor': '#333333', 'signalColor': '#333333', 'signalTextColor': '#1976d2', 'actorBkg': '#e3f2fd', 'actorBorder': '#1976d2', 'actorTextColor': '#000000', 'activationBkgColor': '#e8f5e9', 'noteBkgColor': '#fff3e0', 'noteTextColor': '#000000', 'noteBorderColor': '#f57c00', 'loopTextColor': '#000000', 'labelTextColor': '#1976d2', 'background': '#ffffff'}}}%%
sequenceDiagram
    autonumber
    participant CLI as Client
    participant MH as MembershipHandler
    participant KS as KingbusServer
    participant RAFT as Raft
    participant CLUSTER as RaftCluster
    
    CLI->>MH: POST /members
    MH->>MH: echoCtx.Bind(&args)
    
    alt 非 Leader
        MH->>MH: sendToLeader("POST", req)
        MH-->>CLI: Leader 响应
    else Leader
        MH->>MH: types.NewURLs(peerURL)
        MH->>MH: membership.NewMember(name, peerURLs, adminURLs)
        
        MH->>KS: AddMember(ctx, member)
        
        KS->>CLUSTER: IsReadyToAddNewMember()
        CLUSTER-->>KS: 检查结果
        
        KS->>KS: configure(ctx, ConfChange)
        KS->>RAFT: ProposeConfChange(cc)
        
        Note over RAFT: 等待 ConfChange 应用
        
        RAFT-->>KS: confChangeResponse
        KS-->>MH: members, nil
        
        MH-->>CLI: 200 OK + members
    end
```

### 4.3 请求转发流程

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '12px', 'fontFamily': 'Arial', 'primaryColor': '#e3f2fd', 'primaryTextColor': '#000000', 'primaryBorderColor': '#1976d2', 'lineColor': '#333333', 'signalColor': '#333333', 'signalTextColor': '#1976d2', 'actorBkg': '#e3f2fd', 'actorBorder': '#1976d2', 'actorTextColor': '#000000', 'activationBkgColor': '#e8f5e9', 'noteBkgColor': '#fff3e0', 'noteTextColor': '#000000', 'noteBorderColor': '#f57c00', 'loopTextColor': '#000000', 'labelTextColor': '#1976d2', 'background': '#ffffff'}}}%%
sequenceDiagram
    autonumber
    participant CLI as Client
    participant FOLLOWER as Follower API
    participant LEADER as Leader API
    participant KS as KingbusServer
    
    CLI->>FOLLOWER: PUT /binlog/syncer/start
    
    FOLLOWER->>FOLLOWER: svr.IsLeader() == false
    FOLLOWER->>FOLLOWER: svr.Leader() -> leaderID
    FOLLOWER->>FOLLOWER: cluster.Member(leaderID)
    FOLLOWER->>FOLLOWER: 解析 Leader AdminURL
    
    FOLLOWER->>LEADER: 转发请求到 Leader
    
    LEADER->>KS: 执行操作
    KS-->>LEADER: 结果
    LEADER-->>FOLLOWER: resp
    
    FOLLOWER-->>CLI: 返回 Leader 响应
```

---

## 5. 源码链路树状图

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '12px', 'fontFamily': 'Arial', 'primaryColor': '#e3f2fd', 'primaryTextColor': '#000000', 'primaryBorderColor': '#1976d2', 'lineColor': '#1976d2', 'background': '#ffffff', 'clusterBkg': '#f5f5f5', 'clusterBorder': '#999999', 'edgeLabelBackground': '#ffffff'}}}%%
graph LR
    subgraph "API 模块源码结构"
        
        subgraph "api/api_server.go"
            AS["AdminServer struct<br/>Line: 34-40"]
            AS1["AdminAddr string"]
            AS2["web *echo.Echo"]
            AS3["mh *MembershipHandler"]
            AS4["bs *BinlogSyncerHandler"]
            AS5["bm *BinlogServerHandler"]
            
            ASF1["NewAdminServer()<br/>Line: 66-87"]
            ASF2["Run()<br/>Line: 90-97"]
            ASF3["RegisterMiddleware()<br/>Line: 100-112"]
            ASF4["RegisterURL()<br/>Line: 115-133"]
            ASF5["Stop()<br/>Line: 136-142"]
        end
        
        subgraph "api/membership_handler.go"
            MH["MembershipHandler<br/>Line: 45-49"]
            MH1["GetCluster()<br/>Line: 73-89"]
            MH2["GetMembers()<br/>Line: 92-95"]
            MH3["AddMember()<br/>Line: 122-170"]
            MH4["UpdateMember()<br/>Line: 173-223"]
            MH5["DeleteMember()<br/>Line: 226-273"]
            MH6["UpdateAdminURL()<br/>Line: 98-119"]
            MH7["sendToLeader()<br/>Line: 275-298"]
        end
        
        subgraph "api/binlog_syncer_handler.go"
            BSH["BinlogSyncerHandler<br/>Line: 33-38"]
            BSH1["StartBinlogSyncer()<br/>Line: 41-98"]
            BSH2["StopBinlogSyncer()<br/>Line: 129-148"]
            BSH3["GetBinlogSyncerStatus()<br/>Line: 151-173"]
            BSH4["ProposeSyncerArgs()<br/>Line: 176-187"]
            BSH5["sendToLeader()<br/>Line: 101-126"]
        end
        
        subgraph "api/binlog_server_handler.go"
            BMH["BinlogServerHandler<br/>Line: 16-19"]
            BMH1["StartBinlogServer()<br/>Line: 23-55"]
            BMH2["StopBinlogServer()<br/>Line: 70-75"]
            BMH3["GetBinlogServerStatus()<br/>Line: 58-67"]
        end
        
        AS --> AS1 & AS2 & AS3 & AS4 & AS5
        AS --> ASF1 & ASF2 & ASF3 & ASF4 & ASF5
        
        MH --> MH1 & MH2 & MH3 & MH4 & MH5 & MH6 & MH7
        BSH --> BSH1 & BSH2 & BSH3 & BSH4 & BSH5
        BMH --> BMH1 & BMH2 & BMH3
    end
    
    style AS fill:#e3f2fd,stroke:#1976d2,stroke-width:2px
    style MH fill:#e8f5e9,stroke:#388e3c,stroke-width:2px
    style BSH fill:#fff3e0,stroke:#f57c00,stroke-width:2px
    style BMH fill:#fce4ec,stroke:#c2185b,stroke-width:2px
```

---

## 6. 关键代码路径表

| **功能** | **文件路径** | **行号** | **函数/结构体** |
|---------|-------------|---------|----------------|
| **AdminServer 结构体** | `api/api_server.go` | 34-40 | `AdminServer` |
| **Server 接口** | `api/api_server.go` | 43-63 | `Server` |
| **创建 AdminServer** | `api/api_server.go` | 66-87 | `NewAdminServer()` |
| **启动服务** | `api/api_server.go` | 90-97 | `Run()` |
| **注册中间件** | `api/api_server.go` | 100-112 | `RegisterMiddleware()` |
| **注册路由** | `api/api_server.go` | 115-133 | `RegisterURL()` |
| **停止服务** | `api/api_server.go` | 136-142 | `Stop()` |
| **MembershipHandler** | `api/membership_handler.go` | 45-49 | `MembershipHandler` |
| **获取集群信息** | `api/membership_handler.go` | 73-89 | `GetCluster()` |
| **添加成员** | `api/membership_handler.go` | 122-170 | `AddMember()` |
| **删除成员** | `api/membership_handler.go` | 226-273 | `DeleteMember()` |
| **BinlogSyncerHandler** | `api/binlog_syncer_handler.go` | 33-38 | `BinlogSyncerHandler` |
| **启动 Syncer** | `api/binlog_syncer_handler.go` | 41-98 | `StartBinlogSyncer()` |
| **停止 Syncer** | `api/binlog_syncer_handler.go` | 129-148 | `StopBinlogSyncer()` |
| **BinlogServerHandler** | `api/binlog_server_handler.go` | 16-19 | `BinlogServerHandler` |
| **启动 BinlogServer** | `api/binlog_server_handler.go` | 23-55 | `StartBinlogServer()` |

---

## 7. API 接口详解

### 7.1 集群管理 API

#### GET /members
获取集群所有成员列表。

```bash
curl http://localhost:9595/members
```

**响应**:
```json
{
  "message": "success",
  "data": [
    {
      "ID": "8e9e05c52164694d",
      "name": "node1",
      "peerURLs": ["http://127.0.0.1:2380"],
      "adminURLs": ["http://127.0.0.1:9595"]
    }
  ]
}
```

#### POST /members
添加新成员到集群。

```bash
curl -X POST http://localhost:9595/members \
  -H "Content-Type: application/json" \
  -d '{
    "name": "node2",
    "peer_url": "http://127.0.0.1:2381",
    "admin_url": "http://127.0.0.1:9596"
  }'
```

#### DELETE /members
从集群删除成员。

```bash
curl -X DELETE http://localhost:9595/members \
  -H "Content-Type: application/json" \
  -d '{"name": "node2"}'
```

#### GET /cluster
获取集群完整信息（包含 Leader 标识）。

```bash
curl http://localhost:9595/cluster
```

**响应**:
```json
{
  "message": "success",
  "data": [
    {
      "Id": "8e9e05c52164694d",
      "name": "node1",
      "peerURLs": ["http://127.0.0.1:2380"],
      "adminURLs": ["http://127.0.0.1:9595"],
      "isLeader": true
    }
  ]
}
```

### 7.2 Syncer API

#### PUT /binlog/syncer/start
启动 Syncer。

```bash
curl -X PUT http://localhost:9595/binlog/syncer/start \
  -H "Content-Type: application/json" \
  -d '{
    "mysql_addr": "127.0.0.1:3306",
    "mysql_user": "repl",
    "mysql_password": "repl123",
    "semi_sync": false
  }'
```

#### PUT /binlog/syncer/stop
停止 Syncer。

```bash
curl -X PUT http://localhost:9595/binlog/syncer/stop
```

#### GET /binlog/syncer/status
获取 Syncer 状态。

```bash
curl http://localhost:9595/binlog/syncer/status
```

**响应**:
```json
{
  "message": "success",
  "data": {
    "mysql_addr": "127.0.0.1:3306",
    "mysql_user": "repl",
    "semi_sync": false,
    "status": "running",
    "current_gtid": "uuid:1-1000",
    "last_binlog_file": "mysql-bin.000001",
    "last_file_position": 12345,
    "executed_gtid_set": "uuid:1-1000",
    "purged_gtid_set": ""
  }
}
```

### 7.3 BinlogServer API

#### PUT /binlog/server/start
启动 BinlogServer。

```bash
curl -X PUT http://localhost:9595/binlog/server/start \
  -H "Content-Type: application/json" \
  -d '{
    "addr": "0.0.0.0:3307",
    "user": "kingbus",
    "password": "kingbus123"
  }'
```

#### PUT /binlog/server/stop
停止 BinlogServer。

```bash
curl -X PUT http://localhost:9595/binlog/server/stop
```

#### GET /binlog/server/status
获取 BinlogServer 状态。

```bash
curl http://localhost:9595/binlog/server/status
```

**响应**:
```json
{
  "message": "success",
  "data": {
    "addr": "0.0.0.0:3307",
    "user": "kingbus",
    "status": "running",
    "slaves": [
      {
        "server_id": 100,
        "uuid": "xxx-xxx",
        "hostname": "slave1",
        "state": "DUMPING",
        "connect_time": "2024-01-01T00:00:00Z"
      }
    ],
    "current_gtid": "uuid:1-1000",
    "executed_gtid_set": "uuid:1-1000"
  }
}
```

---

## 8. 中间件

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '12px', 'fontFamily': 'Arial', 'primaryColor': '#e3f2fd', 'primaryTextColor': '#000000', 'primaryBorderColor': '#1976d2', 'lineColor': '#1976d2', 'background': '#ffffff', 'clusterBkg': '#f5f5f5', 'clusterBorder': '#999999', 'edgeLabelBackground': '#ffffff'}}}%%
graph LR
    subgraph "请求处理管道"
        REQ["HTTP Request"] --> MW1["Logger 中间件<br/>记录请求日志"]
        MW1 --> MW2["Recover 中间件<br/>panic 恢复"]
        MW2 --> HANDLER["Handler<br/>业务处理"]
        HANDLER --> RESP["HTTP Response"]
    end
    
    style MW1 fill:#e8f5e9,stroke:#388e3c,stroke-width:2px
    style MW2 fill:#fff3e0,stroke:#f57c00,stroke-width:2px
```

### Logger 中间件日志格式

```json
{
  "time": "2024-01-01T00:00:00.000Z",
  "id": "xxx",
  "remote_ip": "127.0.0.1",
  "host": "localhost:9595",
  "method": "PUT",
  "uri": "/binlog/syncer/start",
  "status": 200,
  "latency": 12345,
  "latency_human": "12.345ms",
  "bytes_in": 123,
  "bytes_out": 456
}
```

---

## 9. 错误处理

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '12px', 'fontFamily': 'Arial', 'primaryColor': '#e3f2fd', 'primaryTextColor': '#000000', 'primaryBorderColor': '#1976d2', 'lineColor': '#1976d2', 'background': '#ffffff', 'clusterBkg': '#f5f5f5', 'clusterBorder': '#999999', 'edgeLabelBackground': '#ffffff'}}}%%
graph TB
    subgraph "错误类型"
        E1["ErrNoLeader<br/>无 Leader"]
        E2["ErrNotLeader<br/>非 Leader 节点"]
        E3["ErrIDExists<br/>成员已存在"]
        E4["ErrIDNotFound<br/>成员不存在"]
        E5["ErrUnhealthy<br/>集群不健康"]
        E6["参数错误<br/>Bind/Check 失败"]
    end
    
    subgraph "HTTP 状态码"
        S200["200 OK<br/>成功"]
        S400["400 Bad Request<br/>参数错误"]
        S404["404 Not Found<br/>资源不存在"]
        S409["409 Conflict<br/>冲突"]
        S410["410 Gone<br/>已删除"]
        S500["500 Internal Error<br/>服务器错误"]
    end
    
    E1 --> S500
    E2 --> S500
    E3 --> S409
    E4 --> S404
    E5 --> S500
    E6 --> S400
    
    style E1 fill:#fce4ec,stroke:#c2185b,stroke-width:2px
    style S200 fill:#e8f5e9,stroke:#388e3c,stroke-width:2px
```

---

## 10. Server 接口定义

```go
// api/api_server.go:43-63
type Server interface {
    // 成员管理
    AddMember(ctx context.Context, memb membership.Member) ([]*membership.Member, error)
    RemoveMember(ctx context.Context, id uint64) ([]*membership.Member, error)
    UpdateMember(ctx context.Context, updateMemb membership.Member) ([]*membership.Member, error)
    
    // 状态查询
    GetIP() string
    IsLeader() bool
    Leader() types.ID
    
    // Raft 操作
    Propose(data []byte) error
    
    // 子服务管理
    StartServer(svrType config.SubServerType, args interface{}) error
    StopServer(svrType config.SubServerType)
    GetServerStatus(svrType config.SubServerType) interface{}
}
```

---

## 11. 依赖关系

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '12px', 'fontFamily': 'Arial', 'primaryColor': '#e3f2fd', 'primaryTextColor': '#000000', 'primaryBorderColor': '#1976d2', 'lineColor': '#1976d2', 'background': '#ffffff', 'clusterBkg': '#f5f5f5', 'clusterBorder': '#999999', 'edgeLabelBackground': '#ffffff'}}}%%
graph TB
    subgraph "API 依赖"
        API["API Module"]
        
        subgraph "外部库"
            ECHO["labstack/echo<br/>Web 框架"]
            ECHOMW["echo/middleware<br/>中间件"]
        end
        
        subgraph "内部模块"
            CONFIG["config<br/>配置结构"]
            MEMBER["raft/membership<br/>集群成员"]
            UTILS["utils<br/>工具函数"]
            LOG["log<br/>日志"]
        end
        
        API --> ECHO & ECHOMW
        API --> CONFIG & MEMBER & UTILS & LOG
    end
    
    style API fill:#e3f2fd,stroke:#1976d2,stroke-width:2px
    style ECHO fill:#e8f5e9,stroke:#388e3c,stroke-width:2px
```

---

## 12. 配置参数

| **参数** | **位置** | **说明** |
|---------|---------|---------|
| `AdminURLs` | `config.yaml` | Admin API 监听地址 |
| `adminAPITimeout` | `api_server.go:30` | API 超时时间 (10s) |

---

## 13. 使用示例

### 完整部署流程

```bash
# 1. 启动 Kingbus 集群 (3 节点)
./kingbus --config node1.yaml &
./kingbus --config node2.yaml &
./kingbus --config node3.yaml &

# 2. 检查集群状态
curl http://localhost:9595/cluster

# 3. 启动 Syncer (连接 MySQL Master)
curl -X PUT http://localhost:9595/binlog/syncer/start \
  -H "Content-Type: application/json" \
  -d '{
    "mysql_addr": "mysql-master:3306",
    "mysql_user": "repl",
    "mysql_password": "repl123"
  }'

# 4. 检查 Syncer 状态
curl http://localhost:9595/binlog/syncer/status

# 5. 启动 BinlogServer (供下游 Slave 连接)
curl -X PUT http://localhost:9595/binlog/server/start \
  -H "Content-Type: application/json" \
  -d '{
    "addr": "0.0.0.0:3307",
    "user": "kingbus",
    "password": "kingbus123"
  }'

# 6. 检查 BinlogServer 状态
curl http://localhost:9595/binlog/server/status
```

