# Kingbus 功能增强设计文档

## 概述

本设计文档描述了对Kingbus系统的三个主要功能增强：

1. **MySQL查询命令透传Master功能** - 支持将复杂查询透传到真实MySQL主库
2. **多元数据存储后端支持** - 支持Redis、MySQL等多种元数据存储后端
3. **分层文件存储架构** - 本地保留最新数据，历史数据上传到S3

## 1. MySQL查询命令透传Master功能

### 1.1 需求分析

当前Kingbus只支持有限的MySQL命令（主要是复制相关的命令），对于一些常见的查询需求无法满足，如`SELECT`查询、`SHOW CREATE TABLE`等。需要增加透传功能，将这些只读查询直接转发到真实的MySQL主库，但不支持DML（INSERT/UPDATE/DELETE）和DDL（CREATE/DROP/ALTER）操作。

### 1.2 架构设计

#### 1.2.1 整体架构

```mermaid
graph TB
    subgraph "Client Connection"
        CLIENT[MySQL Client]
    end
    
    subgraph "Kingbus Cluster"
        subgraph "BinlogServer"
            CONN[MySQL Connection Handler]
            PARSER[SQL Parser]
        end
        
        subgraph "Query Router"
            ROUTER[Query Router]
            PASSTHROUGH[Passthrough Handler]
            LOCAL[Local Handler]
        end
        
        subgraph "Connection Pool"
            POOL[Master Connection Pool]
        end
    end
    
    subgraph "External Systems"
        MASTER[MySQL Master]
    end
    
    CLIENT --> CONN
    CONN --> PARSER
    PARSER --> ROUTER
    ROUTER --> LOCAL
    ROUTER --> PASSTHROUGH
    PASSTHROUGH --> POOL
    POOL --> MASTER
```

#### 1.2.2 核心组件设计

**1. 查询路由器 (Query Router)**

```go
// mysql/query_router.go
package mysql

import (
    "context"
    "database/sql"
    "sync"
    "time"
)

type QueryType int

const (
    QueryTypeLocal QueryType = iota    // 本地处理
    QueryTypePassthrough               // 透传到Master（只读查询）
    QueryTypeNotSupported              // 不支持的操作（DML/DDL）
)

type QueryRouter struct {
    masterPool    *ConnectionPool
    config        *PassthroughConfig
    metrics       *PassthroughMetrics
}

type PassthroughConfig struct {
    Enable              bool          `yaml:"enable"`
    MasterHost          string        `yaml:"master_host"`
    MasterPort          int           `yaml:"master_port"`
    MasterUser          string        `yaml:"master_user"`
    MasterPassword      string        `yaml:"master_password"`
    MaxConnections      int           `yaml:"max_connections"`
    ConnTimeout         time.Duration `yaml:"conn_timeout"`
    QueryTimeout        time.Duration `yaml:"query_timeout"`
    AllowedStatements   []string      `yaml:"allowed_statements"`  // 允许的SQL语句类型
}

func (r *QueryRouter) RouteQuery(ctx context.Context, sql string) (QueryType, error) {
    // 1. 解析SQL语句类型
    stmtType := r.parseStatementType(sql)
    
    // 2. 检查是否为允许的查询类型
    if r.isAllowedStatement(stmtType) {
        return QueryTypePassthrough, nil
    }
    
    // 3. 检查是否为DML/DDL操作
    if r.isDMLOrDDL(stmtType) {
        return QueryTypeNotSupported, ErrOperationNotSupported
    }
    
    // 4. 其他情况尝试本地处理
    return QueryTypeLocal, nil
}

func (r *QueryRouter) isAllowedStatement(stmtType string) bool {
    for _, allowed := range r.config.AllowedStatements {
        if strings.EqualFold(allowed, stmtType) {
            return true
        }
    }
    return false
}

func (r *QueryRouter) isDMLOrDDL(stmtType string) bool {
    dmlDDLStatements := []string{
        "INSERT", "UPDATE", "DELETE",           // DML
        "CREATE", "DROP", "ALTER", "TRUNCATE",  // DDL
    }
    
    for _, stmt := range dmlDDLStatements {
        if strings.EqualFold(stmt, stmtType) {
            return true
        }
    }
    return false
}
```

**2. 连接池管理**

```go
// mysql/connection_pool.go
package mysql

type ConnectionPool struct {
    mu          sync.RWMutex
    connections chan *sql.DB
    config      *PassthroughConfig
    metrics     *PoolMetrics
}

func NewConnectionPool(config *PassthroughConfig) (*ConnectionPool, error) {
    pool := &ConnectionPool{
        connections: make(chan *sql.DB, config.MaxConnections),
        config:      config,
        metrics:     NewPoolMetrics(),
    }
    
    // 预创建连接
    for i := 0; i < config.MaxConnections; i++ {
        conn, err := pool.createConnection()
        if err != nil {
            return nil, err
        }
        pool.connections <- conn
    }
    
    return pool, nil
}

func (p *ConnectionPool) GetConnection(ctx context.Context) (*sql.DB, error) {
    select {
    case conn := <-p.connections:
        if err := conn.PingContext(ctx); err != nil {
            // 连接失效，重新创建
            conn.Close()
            newConn, err := p.createConnection()
            if err != nil {
                return nil, err
            }
            return newConn, nil
        }
        return conn, nil
    case <-ctx.Done():
        return nil, ctx.Err()
    case <-time.After(p.config.ConnTimeout):
        return nil, ErrConnectionTimeout
    }
}

func (p *ConnectionPool) ReturnConnection(conn *sql.DB) {
    select {
    case p.connections <- conn:
    default:
        // 连接池已满，关闭连接
        conn.Close()
    }
}
```

**3. 错误定义**

```go
// mysql/errors.go
package mysql

import "errors"

var (
    ErrOperationNotSupported = errors.New("DML/DDL operations are not supported")
    ErrConnectionTimeout     = errors.New("connection timeout")
    ErrQueryTimeout         = errors.New("query timeout")
)
```

#### 1.2.3 修改现有的命令处理器

```go
// mysql/command.go (修改现有代码)

func (c *Conn) handleQuery(sql string) (err error) {
    // ... 现有代码 ...
    
    // 使用查询路由器判断如何处理
    queryType, err := c.queryRouter.RouteQuery(context.Background(), sql)
    if err != nil {
        return err
    }
    
    switch queryType {
    case QueryTypeLocal:
        return c.handleQueryLocally(sql)
    case QueryTypePassthrough:
        return c.handleQueryPassthrough(sql)
    case QueryTypeNotSupported:
        return c.writeError(ErrOperationNotSupported)
    default:
        return c.writeError(ErrSQLNotSupport)
    }
}

func (c *Conn) handleQueryPassthrough(sql string) error {
    ctx, cancel := context.WithTimeout(context.Background(), c.passthroughConfig.QueryTimeout)
    defer cancel()
    
    // 1. 获取连接
    conn, err := c.masterPool.GetConnection(ctx)
    if err != nil {
        return err
    }
    defer c.masterPool.ReturnConnection(conn)
    
    // 2. 执行查询
    rows, err := conn.QueryContext(ctx, sql)
    if err != nil {
        return err
    }
    defer rows.Close()
    
    // 3. 构建结果集并返回
    result, err := c.buildResultFromRows(rows)
    if err != nil {
        return err
    }
    
    return c.writeResultset(result)
}
```

### 1.3 配置扩展

```yaml
# etc/kingbus.yaml (新增配置)
passthrough:
  enable: true
  master:
    host: "192.168.1.10"
    port: 3306
    user: "kingbus_query"
    password: "password123"
    max_connections: 10
    conn_timeout: "5s"
    query_timeout: "30s"
  
  # 允许透传的SQL语句类型
  allowed_statements:
    - "SELECT"
    - "SHOW"
    - "DESCRIBE" 
    - "DESC"
    - "EXPLAIN"
```

## 2. 多元数据存储后端支持

### 2.1 需求分析

当前Kingbus只支持BoltDB作为元数据存储，需要支持更多存储后端以适应不同的部署环境和性能需求。

### 2.2 架构设计

#### 2.2.1 存储抽象层重构

```go
// storage/meta_storage.go (重构现有接口)

type MetaStorageType string

const (
    MetaStorageBoltDB MetaStorageType = "boltdb"
    MetaStorageRedis  MetaStorageType = "redis"
    MetaStorageMySQL  MetaStorageType = "mysql"
    MetaStorageEtcd   MetaStorageType = "etcd"
)

type MetaStorageConfig struct {
    Type     MetaStorageType `yaml:"type"`
    BoltDB   *BoltDBConfig   `yaml:"boltdb,omitempty"`
    Redis    *RedisConfig    `yaml:"redis,omitempty"`
    MySQL    *MySQLConfig    `yaml:"mysql,omitempty"`
    Etcd     *EtcdConfig     `yaml:"etcd,omitempty"`
}

type BoltDBConfig struct {
    Path string `yaml:"path"`
}

type RedisConfig struct {
    Addrs      []string `yaml:"addrs"`
    Password   string   `yaml:"password"`
    DB         int      `yaml:"db"`
    Prefix     string   `yaml:"prefix"`
    MaxRetries int      `yaml:"max_retries"`
}

type MySQLConfig struct {
    DSN          string `yaml:"dsn"`
    Database     string `yaml:"database"`
    Table        string `yaml:"table"`
    MaxOpenConns int    `yaml:"max_open_conns"`
    MaxIdleConns int    `yaml:"max_idle_conns"`
}

type EtcdConfig struct {
    Endpoints []string `yaml:"endpoints"`
    Prefix    string   `yaml:"prefix"`
    Username  string   `yaml:"username"`
    Password  string   `yaml:"password"`
}
```

#### 2.2.2 Redis存储实现

```go
// storage/redis_meta_storage.go
package storage

import (
    "encoding/json"
    "fmt"
    "strconv"
    "time"
    
    "github.com/go-redis/redis/v8"
    pb "github.com/coreos/etcd/raft/raftpb"
    gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

type RedisMetaStorage struct {
    client redis.UniversalClient
    prefix string
}

func NewRedisMetaStorage(config *RedisConfig) (*RedisMetaStorage, error) {
    var client redis.UniversalClient
    
    if len(config.Addrs) == 1 {
        // 单实例Redis
        client = redis.NewClient(&redis.Options{
            Addr:       config.Addrs[0],
            Password:   config.Password,
            DB:         config.DB,
            MaxRetries: config.MaxRetries,
        })
    } else {
        // Redis Cluster
        client = redis.NewClusterClient(&redis.ClusterOptions{
            Addrs:      config.Addrs,
            Password:   config.Password,
            MaxRetries: config.MaxRetries,
        })
    }
    
    // 测试连接
    ctx := context.Background()
    if err := client.Ping(ctx).Err(); err != nil {
        return nil, fmt.Errorf("redis connection failed: %v", err)
    }
    
    return &RedisMetaStorage{
        client: client,
        prefix: config.Prefix,
    }, nil
}

func (r *RedisMetaStorage) Get(key []byte) ([]byte, error) {
    ctx := context.Background()
    result, err := r.client.Get(ctx, r.keyWithPrefix(key)).Bytes()
    if err == redis.Nil {
        return nil, nil
    }
    return result, err
}

func (r *RedisMetaStorage) Set(key, value []byte) error {
    ctx := context.Background()
    return r.client.Set(ctx, r.keyWithPrefix(key), value, 0).Err()
}

func (r *RedisMetaStorage) Delete(key []byte) error {
    ctx := context.Background()
    return r.client.Del(ctx, r.keyWithPrefix(key)).Err()
}

func (r *RedisMetaStorage) InitialState() (pb.HardState, pb.ConfState, error) {
    // 实现Raft状态的Redis存储
    hardState := pb.HardState{}
    confState := pb.ConfState{}
    
    // 获取HardState
    if data, err := r.Get([]byte("raft_hard_state")); err == nil && data != nil {
        if err := hardState.Unmarshal(data); err != nil {
            return hardState, confState, err
        }
    }
    
    // 获取ConfState
    if data, err := r.Get([]byte("raft_conf_state")); err == nil && data != nil {
        if err := confState.Unmarshal(data); err != nil {
            return hardState, confState, err
        }
    }
    
    return hardState, confState, nil
}

func (r *RedisMetaStorage) SaveHardState(st pb.HardState) error {
    data, err := st.Marshal()
    if err != nil {
        return err
    }
    return r.Set([]byte("raft_hard_state"), data)
}

func (r *RedisMetaStorage) SetGtidSet(flavor string, key string, gtidSet gomysql.GTIDSet) error {
    fullKey := fmt.Sprintf("%s_%s", flavor, key)
    data := gtidSet.Encode()
    return r.Set([]byte(fullKey), data)
}

func (r *RedisMetaStorage) GetGtidSet(flavor string, key string) (gomysql.GTIDSet, error) {
    fullKey := fmt.Sprintf("%s_%s", flavor, key)
    data, err := r.Get([]byte(fullKey))
    if err != nil || data == nil {
        return nil, err
    }
    
    switch flavor {
    case gomysql.MySQLFlavor:
        return gomysql.DecodeMysqlGTIDSet(data)
    case gomysql.MariaDBFlavor:
        return gomysql.DecodeMariadbGTIDSet(data)
    default:
        return nil, fmt.Errorf("unsupported flavor: %s", flavor)
    }
}

func (r *RedisMetaStorage) keyWithPrefix(key []byte) string {
    return r.prefix + string(key)
}

func (r *RedisMetaStorage) Close2() error {
    return r.client.Close()
}
```

#### 2.2.3 MySQL存储实现

```go
// storage/mysql_meta_storage.go
package storage

import (
    "database/sql"
    "encoding/json"
    "fmt"
    
    _ "github.com/go-sql-driver/mysql"
    pb "github.com/coreos/etcd/raft/raftpb"
    gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

type MySQLMetaStorage struct {
    db       *sql.DB
    database string
    table    string
}

const createTableSQL = `
CREATE TABLE IF NOT EXISTS %s (
    id VARCHAR(255) PRIMARY KEY,
    value LONGBLOB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
`

func NewMySQLMetaStorage(config *MySQLConfig) (*MySQLMetaStorage, error) {
    db, err := sql.Open("mysql", config.DSN)
    if err != nil {
        return nil, err
    }
    
    db.SetMaxOpenConns(config.MaxOpenConns)
    db.SetMaxIdleConns(config.MaxIdleConns)
    
    // 测试连接
    if err := db.Ping(); err != nil {
        return nil, err
    }
    
    storage := &MySQLMetaStorage{
        db:       db,
        database: config.Database,
        table:    config.Table,
    }
    
    // 创建表
    if err := storage.createTable(); err != nil {
        return nil, err
    }
    
    return storage, nil
}

func (m *MySQLMetaStorage) createTable() error {
    query := fmt.Sprintf(createTableSQL, m.table)
    _, err := m.db.Exec(query)
    return err
}

func (m *MySQLMetaStorage) Get(key []byte) ([]byte, error) {
    query := fmt.Sprintf("SELECT value FROM %s WHERE id = ?", m.table)
    var value []byte
    err := m.db.QueryRow(query, string(key)).Scan(&value)
    if err == sql.ErrNoRows {
        return nil, nil
    }
    return value, err
}

func (m *MySQLMetaStorage) Set(key, value []byte) error {
    query := fmt.Sprintf(`
        INSERT INTO %s (id, value) VALUES (?, ?) 
        ON DUPLICATE KEY UPDATE value = VALUES(value), updated_at = CURRENT_TIMESTAMP
    `, m.table)
    _, err := m.db.Exec(query, string(key), value)
    return err
}

func (m *MySQLMetaStorage) Delete(key []byte) error {
    query := fmt.Sprintf("DELETE FROM %s WHERE id = ?", m.table)
    _, err := m.db.Exec(query, string(key))
    return err
}

// 实现其他MetaStorage接口方法...
```

#### 2.2.4 存储工厂

```go
// storage/factory.go
package storage

func NewMetaStorage(config *MetaStorageConfig) (MetaStorage, error) {
    switch config.Type {
    case MetaStorageBoltDB:
        if config.BoltDB == nil {
            return nil, fmt.Errorf("BoltDB config is required")
        }
        return NewMetaStore(config.BoltDB.Path)
        
    case MetaStorageRedis:
        if config.Redis == nil {
            return nil, fmt.Errorf("Redis config is required")
        }
        return NewRedisMetaStorage(config.Redis)
        
    case MetaStorageMySQL:
        if config.MySQL == nil {
            return nil, fmt.Errorf("MySQL config is required")
        }
        return NewMySQLMetaStorage(config.MySQL)
        
    case MetaStorageEtcd:
        if config.Etcd == nil {
            return nil, fmt.Errorf("Etcd config is required")
        }
        return NewEtcdMetaStorage(config.Etcd)
        
    default:
        return nil, fmt.Errorf("unsupported meta storage type: %s", config.Type)
    }
}
```

### 2.3 配置扩展

```yaml
# etc/kingbus.yaml (元数据存储配置)
meta_storage:
  type: "redis"  # boltdb, redis, mysql, etcd
  
  # BoltDB配置
  boltdb:
    path: "/data/kingbus/meta"
  
  # Redis配置
  redis:
    addrs:
      - "127.0.0.1:6379"
      - "127.0.0.1:6380"
      - "127.0.0.1:6381"
    password: "redis_password"
    db: 0
    prefix: "kingbus:meta:"
    max_retries: 3
  
  # MySQL配置
  mysql:
    dsn: "kingbus:password@tcp(127.0.0.1:3306)/kingbus_meta?charset=utf8mb4&parseTime=true"
    database: "kingbus_meta"
    table: "metadata"
    max_open_conns: 10
    max_idle_conns: 5
  
  # Etcd配置
  etcd:
    endpoints:
      - "127.0.0.1:2379"
      - "127.0.0.1:2380"
      - "127.0.0.1:2381"
    prefix: "/kingbus/meta/"
    username: "kingbus"
    password: "etcd_password"
```

## 3. 分层文件存储架构

### 3.1 需求分析

当前所有binlog数据都存储在本地磁盘，需要实现分层存储：
- **热数据**：最近的binlog数据保留在本地SSD
- **冷数据**：历史binlog数据上传到S3等对象存储

### 3.2 架构设计

#### 3.2.1 分层存储架构

```mermaid
graph TB
    subgraph "Application Layer"
        APP[Kingbus Application]
    end
    
    subgraph "Storage Abstraction Layer"
        STORAGE[Tiered Storage Manager]
        POLICY[Lifecycle Policy Engine]
        CACHE[Local Cache Manager]
    end
    
    subgraph "Hot Storage Tier"
        LOCAL[Local SSD Storage]
        RECENT[Recent Segments]
    end
    
    subgraph "Cold Storage Tier"
        S3[S3 Compatible Storage]
        ARCHIVE[Archived Segments]
    end
    
    subgraph "Metadata Layer"
        META[Segment Metadata Store]
        INDEX[Global Index]
    end
    
    APP --> STORAGE
    STORAGE --> POLICY
    STORAGE --> CACHE
    STORAGE --> LOCAL
    STORAGE --> S3
    POLICY --> META
    CACHE --> LOCAL
    META --> INDEX
```

#### 3.2.2 核心组件设计

**1. 分层存储管理器**

```go
// storage/tiered_storage.go
package storage

import (
    "context"
    "fmt"
    "sync"
    "time"
    
    "github.com/aws/aws-sdk-go/service/s3"
)

type TieredStorage struct {
    config          *TieredStorageConfig
    localStorage    *LocalStorage
    remoteStorage   RemoteStorage
    metaStorage     MetaStorage
    lifecyclePolicy *LifecyclePolicy
    
    mu              sync.RWMutex
    segmentIndex    map[uint64]*SegmentLocation
}

type TieredStorageConfig struct {
    LocalConfig  *LocalStorageConfig  `yaml:"local"`
    RemoteConfig *RemoteStorageConfig `yaml:"remote"`
    Policy       *LifecyclePolicyConfig `yaml:"policy"`
}

type LocalStorageConfig struct {
    Path            string `yaml:"path"`
    MaxSizeGB       int    `yaml:"max_size_gb"`
    ReserveSegments int    `yaml:"reserve_segments"`
}

type RemoteStorageConfig struct {
    Type        string      `yaml:"type"`  // s3, minio, oss
    S3Config    *S3Config   `yaml:"s3,omitempty"`
    MinioConfig *MinioConfig `yaml:"minio,omitempty"`
}

type S3Config struct {
    Region          string `yaml:"region"`
    Bucket          string `yaml:"bucket"`
    Prefix          string `yaml:"prefix"`
    AccessKeyID     string `yaml:"access_key_id"`
    SecretAccessKey string `yaml:"secret_access_key"`
    Endpoint        string `yaml:"endpoint,omitempty"`
    ForcePathStyle  bool   `yaml:"force_path_style"`
}

type LifecyclePolicyConfig struct {
    HotRetentionHours  int `yaml:"hot_retention_hours"`
    ColdRetentionDays  int `yaml:"cold_retention_days"`
    ArchiveAfterDays   int `yaml:"archive_after_days"`
    CheckIntervalHours int `yaml:"check_interval_hours"`
}

type SegmentLocation struct {
    SegmentID   uint64
    FirstIndex  uint64
    LastIndex   uint64
    Tier        StorageTier
    LocalPath   string
    RemotePath  string
    Size        int64
    CreatedAt   time.Time
    AccessedAt  time.Time
    ArchivedAt  *time.Time
}

type StorageTier int

const (
    TierHot StorageTier = iota
    TierCold
    TierArchived
)

func NewTieredStorage(config *TieredStorageConfig, metaStorage MetaStorage) (*TieredStorage, error) {
    // 初始化本地存储
    localStorage, err := NewLocalStorage(config.LocalConfig)
    if err != nil {
        return nil, err
    }
    
    // 初始化远程存储
    remoteStorage, err := NewRemoteStorage(config.RemoteConfig)
    if err != nil {
        return nil, err
    }
    
    // 初始化生命周期策略
    lifecyclePolicy := NewLifecyclePolicy(config.Policy)
    
    ts := &TieredStorage{
        config:          config,
        localStorage:    localStorage,
        remoteStorage:   remoteStorage,
        metaStorage:     metaStorage,
        lifecyclePolicy: lifecyclePolicy,
        segmentIndex:    make(map[uint64]*SegmentLocation),
    }
    
    // 加载现有段的位置信息
    if err := ts.loadSegmentIndex(); err != nil {
        return nil, err
    }
    
    // 启动生命周期管理
    go ts.runLifecycleManager()
    
    return ts, nil
}

func (ts *TieredStorage) SaveRaftEntries(entries []raftpb.Entry) error {
    // 写入本地存储
    err := ts.localStorage.SaveRaftEntries(entries)
    if err != nil {
        return err
    }
    
    // 更新段索引
    ts.updateSegmentIndex(entries)
    
    return nil
}

func (ts *TieredStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
    var entries []raftpb.Entry
    
    for index := lo; index < hi; index++ {
        entry, err := ts.getEntry(index)
        if err != nil {
            return nil, err
        }
        
        entries = append(entries, *entry)
        
        // 检查大小限制
        if maxSize > 0 && ts.calculateSize(entries) >= maxSize {
            break
        }
    }
    
    return entries, nil
}

func (ts *TieredStorage) getEntry(index uint64) (*raftpb.Entry, error) {
    // 1. 查找段位置
    location := ts.findSegmentLocation(index)
    if location == nil {
        return nil, ErrEntryNotFound
    }
    
    // 2. 根据存储层级读取
    switch location.Tier {
    case TierHot:
        return ts.localStorage.getEntry(index)
    case TierCold, TierArchived:
        return ts.getEntryFromRemote(index, location)
    default:
        return nil, ErrInvalidStorageTier
    }
}

func (ts *TieredStorage) getEntryFromRemote(index uint64, location *SegmentLocation) (*raftpb.Entry, error) {
    // 1. 检查本地缓存
    if entry := ts.localStorage.getCachedEntry(index); entry != nil {
        return entry, nil
    }
    
    // 2. 从远程存储下载段文件
    segmentData, err := ts.remoteStorage.DownloadSegment(location.RemotePath)
    if err != nil {
        return nil, err
    }
    
    // 3. 解析段文件并缓存
    segment, err := ts.parseSegmentData(segmentData)
    if err != nil {
        return nil, err
    }
    
    // 4. 缓存热点数据到本地
    ts.localStorage.cacheSegment(segment)
    
    // 5. 返回请求的条目
    return segment.getEntry(index), nil
}
```

**2. 生命周期策略管理**

```go
// storage/lifecycle_policy.go
package storage

type LifecyclePolicy struct {
    config *LifecyclePolicyConfig
}

func (lp *LifecyclePolicy) ShouldMoveToColStorage(segment *SegmentLocation) bool {
    hotRetention := time.Duration(lp.config.HotRetentionHours) * time.Hour
    return time.Since(segment.CreatedAt) > hotRetention
}

func (lp *LifecyclePolicy) ShouldArchive(segment *SegmentLocation) bool {
    archiveThreshold := time.Duration(lp.config.ArchiveAfterDays) * 24 * time.Hour
    return time.Since(segment.CreatedAt) > archiveThreshold
}

func (lp *LifecyclePolicy) ShouldDelete(segment *SegmentLocation) bool {
    if segment.ArchivedAt == nil {
        return false
    }
    
    retention := time.Duration(lp.config.ColdRetentionDays) * 24 * time.Hour
    return time.Since(*segment.ArchivedAt) > retention
}

func (ts *TieredStorage) runLifecycleManager() {
    ticker := time.NewTicker(time.Duration(ts.lifecyclePolicy.config.CheckIntervalHours) * time.Hour)
    defer ticker.Stop()
    
    for {
        select {
        case <-ticker.C:
            ts.executeLifecyclePolicy()
        }
    }
}

func (ts *TieredStorage) executeLifecyclePolicy() {
    ts.mu.RLock()
    segments := make([]*SegmentLocation, 0, len(ts.segmentIndex))
    for _, segment := range ts.segmentIndex {
        segments = append(segments, segment)
    }
    ts.mu.RUnlock()
    
    for _, segment := range segments {
        // 1. 检查是否需要移动到冷存储
        if segment.Tier == TierHot && ts.lifecyclePolicy.ShouldMoveToColStorage(segment) {
            ts.moveToColStorage(segment)
        }
        
        // 2. 检查是否需要归档
        if segment.Tier == TierCold && ts.lifecyclePolicy.ShouldArchive(segment) {
            ts.archiveSegment(segment)
        }
        
        // 3. 检查是否需要删除
        if segment.Tier == TierArchived && ts.lifecyclePolicy.ShouldDelete(segment) {
            ts.deleteSegment(segment)
        }
    }
}
```

**3. S3兼容存储实现**

```go
// storage/s3_storage.go
package storage

import (
    "bytes"
    "context"
    "io"
    
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/credentials"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/s3"
)

type S3Storage struct {
    client *s3.S3
    bucket string
    prefix string
}

func NewS3Storage(config *S3Config) (*S3Storage, error) {
    awsConfig := &aws.Config{
        Region: aws.String(config.Region),
        Credentials: credentials.NewStaticCredentials(
            config.AccessKeyID,
            config.SecretAccessKey,
            "",
        ),
    }
    
    if config.Endpoint != "" {
        awsConfig.Endpoint = aws.String(config.Endpoint)
        awsConfig.S3ForcePathStyle = aws.Bool(config.ForcePathStyle)
    }
    
    sess, err := session.NewSession(awsConfig)
    if err != nil {
        return nil, err
    }
    
    return &S3Storage{
        client: s3.New(sess),
        bucket: config.Bucket,
        prefix: config.Prefix,
    }, nil
}

func (s *S3Storage) UploadSegment(segmentPath string, data []byte) error {
    key := s.prefix + segmentPath
    
    _, err := s.client.PutObject(&s3.PutObjectInput{
        Bucket:        aws.String(s.bucket),
        Key:           aws.String(key),
        Body:          bytes.NewReader(data),
        ContentLength: aws.Int64(int64(len(data))),
        ContentType:   aws.String("application/octet-stream"),
        Metadata: map[string]*string{
            "kingbus-segment": aws.String("true"),
            "upload-time":     aws.String(time.Now().Format(time.RFC3339)),
        },
    })
    
    return err
}

func (s *S3Storage) DownloadSegment(segmentPath string) ([]byte, error) {
    key := s.prefix + segmentPath
    
    result, err := s.client.GetObject(&s3.GetObjectInput{
        Bucket: aws.String(s.bucket),
        Key:    aws.String(key),
    })
    if err != nil {
        return nil, err
    }
    defer result.Body.Close()
    
    return io.ReadAll(result.Body)
}

func (s *S3Storage) DeleteSegment(segmentPath string) error {
    key := s.prefix + segmentPath
    
    _, err := s.client.DeleteObject(&s3.DeleteObjectInput{
        Bucket: aws.String(s.bucket),
        Key:    aws.String(key),
    })
    
    return err
}

func (s *S3Storage) ListSegments(prefix string) ([]string, error) {
    fullPrefix := s.prefix + prefix
    
    result, err := s.client.ListObjectsV2(&s3.ListObjectsV2Input{
        Bucket: aws.String(s.bucket),
        Prefix: aws.String(fullPrefix),
    })
    if err != nil {
        return nil, err
    }
    
    var segments []string
    for _, obj := range result.Contents {
        segments = append(segments, *obj.Key)
    }
    
    return segments, nil
}
```

### 3.3 配置扩展

```yaml
# etc/kingbus.yaml (分层存储配置)
storage:
  type: "tiered"  # disk, tiered
  
  # 分层存储配置
  tiered:
    # 本地存储配置
    local:
      path: "/data/kingbus/hot"
      max_size_gb: 100
      reserve_segments: 10
    
    # 远程存储配置
    remote:
      type: "s3"  # s3, minio, oss
      s3:
        region: "us-west-2"
        bucket: "kingbus-binlog-archive"
        prefix: "cluster-1/"
        access_key_id: "AKIAIOSFODNN7EXAMPLE"
        secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        endpoint: ""  # 留空使用AWS S3，或设置为MinIO等兼容端点
        force_path_style: false
    
    # 生命周期策略
    policy:
      hot_retention_hours: 24      # 热数据保留24小时
      cold_retention_days: 30      # 冷数据保留30天
      archive_after_days: 7        # 7天后归档
      check_interval_hours: 1      # 每小时检查一次
```

## 4. 实施计划

### 4.1 阶段一：MySQL查询透传功能

**时间估计：2-3周**

1. **Week 1**：
   - 设计并实现查询路由器
   - 实现连接池管理
   - 添加基本的透传功能

2. **Week 2**：
   - 完善SQL解析和路由逻辑
   - 添加安全控制（只读查询验证）
   - 完善错误处理和监控

3. **Week 3**：
   - 集成测试
   - 性能优化
   - 文档编写

### 4.2 阶段二：多元数据存储后端

**时间估计：3-4周**

1. **Week 1**：
   - 重构存储抽象层
   - 实现Redis元数据存储

2. **Week 2**：
   - 实现MySQL元数据存储
   - 实现Etcd元数据存储

3. **Week 3**：
   - 实现存储工厂和配置管理
   - 数据迁移工具

4. **Week 4**：
   - 集成测试
   - 性能对比测试
   - 文档编写

### 4.3 阶段三：分层文件存储

**时间估计：4-5周**

1. **Week 1-2**：
   - 设计分层存储架构
   - 实现本地存储管理器
   - 实现S3存储后端

2. **Week 3**：
   - 实现生命周期策略引擎
   - 实现数据迁移逻辑

3. **Week 4**：
   - 实现缓存机制
   - 性能优化

4. **Week 5**：
   - 集成测试
   - 数据一致性验证
   - 文档编写

## 5. 风险评估与缓解措施

### 5.1 技术风险

1. **数据一致性风险**
   - **风险**：分层存储可能导致数据不一致
   - **缓解**：实现严格的元数据管理和校验机制

2. **性能影响**
   - **风险**：透传查询可能影响整体性能
   - **缓解**：实现连接池和限流机制，只支持只读查询

3. **存储后端故障**
   - **风险**：元数据存储后端故障影响服务
   - **缓解**：实现多副本和故障转移机制

### 5.2 运维风险

1. **配置复杂性**
   - **风险**：配置项增多，运维复杂度提升
   - **缓解**：提供配置模板和验证工具

2. **监控复杂性**
   - **风险**：需要监控多个存储后端
   - **缓解**：统一监控接口和告警机制

## 6. 监控与运维

### 6.1 关键指标

1. **透传查询指标**：
   - `kingbus_passthrough_queries_total`
   - `kingbus_passthrough_query_duration`
   - `kingbus_passthrough_errors_total`
   - `kingbus_master_connection_pool_size`

2. **元数据存储指标**：
   - `kingbus_meta_storage_operations_total`
   - `kingbus_meta_storage_operation_duration`
   - `kingbus_meta_storage_errors_total`

3. **分层存储指标**：
   - `kingbus_tiered_storage_hot_segments`
   - `kingbus_tiered_storage_cold_segments`
   - `kingbus_tiered_storage_migration_duration`
   - `kingbus_s3_upload_duration`

### 6.2 告警规则

```yaml
# prometheus告警规则
groups:
- name: kingbus_enhanced_features
  rules:
  - alert: PassthroughQueryHighErrorRate
    expr: rate(kingbus_passthrough_errors_total[5m]) > 0.1
    for: 2m
    labels:
      severity: warning
    annotations:
      summary: "High error rate in passthrough queries"
  
  - alert: MetaStorageConnectionFailed
    expr: kingbus_meta_storage_connection_status == 0
    for: 1m
    labels:
      severity: critical
    annotations:
      summary: "Meta storage connection failed"
  
  - alert: S3UploadFailed
    expr: increase(kingbus_s3_upload_errors_total[10m]) > 5
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "Multiple S3 upload failures detected"
```

## 7. 总结

本设计文档详细描述了Kingbus系统的三个主要功能增强：

1. **MySQL查询透传**：通过查询路由器和连接池，实现只读查询（SELECT、SHOW等）的透传处理，不支持DML/DDL操作
2. **多元数据存储**：支持Redis、MySQL、Etcd等多种元数据存储后端
3. **分层文件存储**：实现本地热数据和远程冷数据的分层存储架构

这些增强功能将显著提升Kingbus的灵活性、可扩展性和成本效益，使其能够更好地适应各种企业级部署场景。查询透传功能专注于只读操作，确保了数据安全性和系统稳定性。
