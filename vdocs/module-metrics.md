# Kingbus Metrics 模块分析

## 1. 模块概述

**Metrics** 模块负责 Kingbus 的监控指标收集与暴露，采用双层架构：使用 `go-metrics` 库进行内部指标收集，通过 `PrometheusServer` 将指标转换并暴露给 Prometheus 抓取。

### 核心职责

- 收集 Syncer、Storage、BinlogServer 的运行时指标
- 定期将 go-metrics 指标同步到 Prometheus
- 提供 HTTP `/metrics` 端点供 Prometheus 抓取
- 支持 Meter、Gauge、Histogram 等多种指标类型

---

## 2. 架构图

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '12px', 'fontFamily': 'Arial', 'primaryColor': '#e3f2fd', 'primaryTextColor': '#000000', 'primaryBorderColor': '#1976d2', 'lineColor': '#1976d2', 'background': '#ffffff', 'clusterBkg': '#f5f5f5', 'clusterBorder': '#999999', 'edgeLabelBackground': '#ffffff'}}}%%
graph TB
    subgraph "Kingbus 指标采集"
        subgraph "Server 指标"
            SM["server/metrics.go"]
            SM1["syncer_eps"]
            SM2["apply_eps"]
            SM3["propose_channel_size"]
        end
        
        subgraph "Storage 指标"
            STM["storage/metrics.go"]
            STM1["read_tps"]
            STM2["read_throughput"]
            STM3["read_latency"]
            STM4["write_tps"]
            STM5["write_throughput"]
            STM6["write_latency"]
        end
        
        subgraph "Slave 指标 (动态)"
            SLM["mysql/command.go"]
            SLM1["slave_eps_{serverID}"]
            SLM2["slave_throughput_{serverID}"]
        end
    end
    
    subgraph "go-metrics Registry"
        REG["metrics.DefaultRegistry<br/>全局注册表"]
    end
    
    subgraph "Prometheus 转换层"
        PS["PrometheusServer<br/>server/prometheus.go"]
        
        subgraph "转换后指标"
            PG["Prometheus Gauges<br/>kingbus_metrics_xxx"]
        end
    end
    
    subgraph "外部系统"
        PROM["Prometheus Server<br/>抓取 /metrics"]
        GRAF["Grafana<br/>可视化"]
    end
    
    SM --> SM1 & SM2 & SM3
    STM --> STM1 & STM2 & STM3 & STM4 & STM5 & STM6
    SLM --> SLM1 & SLM2
    
    SM1 & SM2 & SM3 --> REG
    STM1 & STM2 & STM3 & STM4 & STM5 & STM6 --> REG
    SLM1 & SLM2 --> REG
    
    REG -->|"定期同步"| PS
    PS --> PG
    PG -->|"HTTP /metrics"| PROM
    PROM --> GRAF
    
    style REG fill:#e3f2fd,stroke:#1976d2,stroke-width:2px
    style PS fill:#e8f5e9,stroke:#388e3c,stroke-width:2px
    style PROM fill:#fce4ec,stroke:#c2185b,stroke-width:2px
```

---

## 3. 时序图

### 3.1 指标注册与更新流程

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '12px', 'fontFamily': 'Arial', 'primaryColor': '#e3f2fd', 'primaryTextColor': '#000000', 'primaryBorderColor': '#1976d2', 'lineColor': '#333333', 'signalColor': '#333333', 'signalTextColor': '#1976d2', 'actorBkg': '#e3f2fd', 'actorBorder': '#1976d2', 'actorTextColor': '#000000', 'activationBkgColor': '#e8f5e9', 'noteBkgColor': '#fff3e0', 'noteTextColor': '#000000', 'noteBorderColor': '#f57c00', 'loopTextColor': '#000000', 'labelTextColor': '#1976d2', 'background': '#ffffff'}}}%%
sequenceDiagram
    autonumber
    participant INIT as init()
    participant REG as metrics.Registry
    participant SYN as Syncer
    participant STORE as Storage
    participant PS as PrometheusServer
    participant PROM as Prometheus
    
    Note over INIT: 程序启动时
    
    INIT->>REG: metrics.Register("syncer_eps", syncerEps)
    INIT->>REG: metrics.Register("read_tps", readTps)
    INIT->>REG: metrics.Register("write_tps", writeTps)
    Note over REG: 更多指标注册...
    
    Note over SYN,STORE: 运行时更新
    
    loop Syncer 处理事件
        SYN->>SYN: syncerEps.Mark(1)
        SYN->>SYN: proposeChannelSize.Update(len)
    end
    
    loop Storage 读写操作
        STORE->>STORE: readTps.Mark(count)
        STORE->>STORE: writeTps.Mark(count)
        STORE->>STORE: readLatency.Update(ms)
        STORE->>STORE: writeLatency.Update(ms)
    end
    
    Note over PS: 定时器触发 (每秒)
    
    loop 定期同步
        PS->>PS: updatePrometheusMetricsOnce()
        PS->>REG: registry.Each(callback)
        REG-->>PS: 遍历所有指标
        
        alt Meter 类型
            PS->>PS: gaugeFromNameAndValue(name, Rate1)
        else Histogram 类型
            PS->>PS: mean, p95, max
        else Gauge 类型
            PS->>PS: Value()
        end
    end
    
    PROM->>PS: GET /metrics
    PS-->>PROM: Prometheus 格式指标
```

### 3.2 PrometheusServer 生命周期

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '12px', 'fontFamily': 'Arial', 'primaryColor': '#e3f2fd', 'primaryTextColor': '#000000', 'primaryBorderColor': '#1976d2', 'lineColor': '#333333', 'signalColor': '#333333', 'signalTextColor': '#1976d2', 'actorBkg': '#e3f2fd', 'actorBorder': '#1976d2', 'actorTextColor': '#000000', 'activationBkgColor': '#e8f5e9', 'noteBkgColor': '#fff3e0', 'noteTextColor': '#000000', 'noteBorderColor': '#f57c00', 'loopTextColor': '#000000', 'labelTextColor': '#1976d2', 'background': '#ffffff'}}}%%
sequenceDiagram
    autonumber
    participant KS as KingbusServer
    participant PS as PrometheusServer
    participant HTTP as HTTP Server
    participant TIMER as Timer
    
    KS->>KS: startPrometheus(addr)
    KS->>PS: NewPrometheusServer(addr, registry, promRegistry, interval)
    
    Note over PS: prometheus.go:35-57
    
    PS->>PS: 初始化 HTTP Mux
    PS->>PS: 注册 /metrics Handler
    PS->>PS: 创建 Timer (1s)
    
    KS->>PS: Run()
    
    par HTTP 服务
        PS->>HTTP: server.ListenAndServe()
        Note over HTTP: 监听 metrics 端口
    and 指标同步
        PS->>PS: updatePrometheusMetrics()
        loop 每秒
            TIMER-->>PS: 触发
            PS->>PS: updatePrometheusMetricsOnce()
        end
    end
    
    KS->>PS: Stop()
    PS->>PS: cancel()
    PS->>TIMER: Stop()
    PS->>HTTP: Shutdown()
```

---

## 4. 源码链路树状图

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '12px', 'fontFamily': 'Arial', 'primaryColor': '#e3f2fd', 'primaryTextColor': '#000000', 'primaryBorderColor': '#1976d2', 'lineColor': '#1976d2', 'background': '#ffffff', 'clusterBkg': '#f5f5f5', 'clusterBorder': '#999999', 'edgeLabelBackground': '#ffffff'}}}%%
graph LR
    subgraph "Metrics 模块源码结构"
        
        subgraph "Server 指标"
            SM["server/metrics.go"]
            SM1["syncerEps<br/>Line: 8"]
            SM2["applyEps<br/>Line: 9"]
            SM3["proposeChannelSize<br/>Line: 11"]
            SM4["init()<br/>Line: 14-18"]
        end
        
        subgraph "Storage 指标"
            STM["storage/metrics.go"]
            STM1["readTps<br/>Line: 9"]
            STM2["readThroughput<br/>Line: 10"]
            STM3["readLatency<br/>Line: 11"]
            STM4["writeTps<br/>Line: 13"]
            STM5["writeThroughput<br/>Line: 14"]
            STM6["writeLatency<br/>Line: 15"]
            STM7["init()<br/>Line: 18-26"]
        end
        
        subgraph "Prometheus Server"
            PS["server/prometheus.go"]
            PS1["PrometheusServer struct<br/>Line: 18-31"]
            PS2["NewPrometheusServer()<br/>Line: 35-57"]
            PS3["Run()<br/>Line: 84-90"]
            PS4["Stop()<br/>Line: 93-97"]
            PS5["updatePrometheusMetrics()<br/>Line: 99-109"]
            PS6["updatePrometheusMetricsOnce()<br/>Line: 111-134"]
            PS7["gaugeFromNameAndValue()<br/>Line: 67-81"]
            PS8["flattenKey()<br/>Line: 59-65"]
        end
        
        SM --> SM1 & SM2 & SM3 & SM4
        STM --> STM1 & STM2 & STM3 & STM4 & STM5 & STM6 & STM7
        PS --> PS1 & PS2 & PS3 & PS4 & PS5 & PS6 & PS7 & PS8
    end
    
    style SM fill:#e3f2fd,stroke:#1976d2,stroke-width:2px
    style STM fill:#e8f5e9,stroke:#388e3c,stroke-width:2px
    style PS fill:#fff3e0,stroke:#f57c00,stroke-width:2px
```

---

## 5. 关键代码路径表

| **功能** | **文件路径** | **行号** | **函数/变量** |
|---------|-------------|---------|--------------|
| **PrometheusServer 结构体** | `server/prometheus.go` | 18-31 | `PrometheusServer` |
| **创建 PrometheusServer** | `server/prometheus.go` | 35-57 | `NewPrometheusServer()` |
| **启动 Server** | `server/prometheus.go` | 84-90 | `Run()` |
| **停止 Server** | `server/prometheus.go` | 93-97 | `Stop()` |
| **更新指标** | `server/prometheus.go` | 111-134 | `updatePrometheusMetricsOnce()` |
| **创建 Gauge** | `server/prometheus.go` | 67-81 | `gaugeFromNameAndValue()` |
| **Syncer EPS** | `server/metrics.go` | 8 | `syncerEps` |
| **Apply EPS** | `server/metrics.go` | 9 | `applyEps` |
| **Propose Channel Size** | `server/metrics.go` | 11 | `proposeChannelSize` |
| **Read TPS** | `storage/metrics.go` | 9 | `readTps` |
| **Read Throughput** | `storage/metrics.go` | 10 | `readThroughput` |
| **Read Latency** | `storage/metrics.go` | 11 | `readLatency` |
| **Write TPS** | `storage/metrics.go` | 13 | `writeTps` |
| **Write Throughput** | `storage/metrics.go` | 14 | `writeThroughput` |
| **Write Latency** | `storage/metrics.go` | 15 | `writeLatency` |
| **Slave EPS** | `mysql/command.go` | 519 | 动态创建 |
| **Slave Throughput** | `mysql/command.go` | 520 | 动态创建 |

---

## 6. 指标详解

### 6.1 所有指标一览

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '12px', 'fontFamily': 'Arial', 'primaryColor': '#e3f2fd', 'primaryTextColor': '#000000', 'primaryBorderColor': '#1976d2', 'lineColor': '#1976d2', 'background': '#ffffff', 'clusterBkg': '#f5f5f5', 'clusterBorder': '#999999', 'edgeLabelBackground': '#ffffff'}}}%%
graph TB
    subgraph "Kingbus 指标全览"
        subgraph "Syncer 指标"
            S1["syncer_eps<br/>Meter<br/>事件处理速率"]
            S2["propose_channel_size<br/>Gauge<br/>待提交队列大小"]
        end
        
        subgraph "Apply 指标"
            A1["apply_eps<br/>Meter<br/>状态机应用速率"]
        end
        
        subgraph "Storage 读指标"
            R1["read_tps<br/>Meter<br/>读操作 TPS"]
            R2["read_throughput<br/>Meter<br/>读吞吐量 (bytes/s)"]
            R3["read_latency<br/>Histogram<br/>读延迟 (ms)"]
        end
        
        subgraph "Storage 写指标"
            W1["write_tps<br/>Meter<br/>写操作 TPS"]
            W2["write_throughput<br/>Meter<br/>写吞吐量 (bytes/s)"]
            W3["write_latency<br/>Histogram<br/>写延迟 (ms)"]
        end
        
        subgraph "Slave 指标 (动态)"
            SL1["slave_eps_{id}<br/>Meter<br/>每个 Slave 的 EPS"]
            SL2["slave_throughput_{id}<br/>Meter<br/>每个 Slave 的吞吐量"]
        end
    end
    
    style S1 fill:#e3f2fd,stroke:#1976d2,stroke-width:2px
    style R1 fill:#e8f5e9,stroke:#388e3c,stroke-width:2px
    style W1 fill:#fff3e0,stroke:#f57c00,stroke-width:2px
    style SL1 fill:#fce4ec,stroke:#c2185b,stroke-width:2px
```

### 6.2 指标类型说明

| **go-metrics 类型** | **Prometheus 转换** | **说明** |
|-------------------|-------------------|---------|
| `Meter` | Gauge (Rate1) | 速率类指标，取最近1分钟速率 |
| `Gauge` | Gauge (Value) | 瞬时值指标 |
| `GaugeFloat64` | Gauge (Value) | 浮点瞬时值 |
| `Histogram` | Gauge (mean/p95/max) | 分布类指标，转为多个 Gauge |
| `Counter` | Gauge (Count) | 计数器 |
| `Timer` | Gauge (Rate1) | 计时器，取速率 |

---

## 7. Prometheus 指标格式

### 转换后的指标命名

```
kingbus_metrics_{原指标名}
```

### 示例输出

```prometheus
# HELP kingbus_metrics_syncer_eps syncer_eps
# TYPE kingbus_metrics_syncer_eps gauge
kingbus_metrics_syncer_eps 1234.56

# HELP kingbus_metrics_read_latency_mean read_latency.mean
# TYPE kingbus_metrics_read_latency_mean gauge
kingbus_metrics_read_latency_mean 5.23

# HELP kingbus_metrics_read_latency_p95 read_latency.p95
# TYPE kingbus_metrics_read_latency_p95 gauge
kingbus_metrics_read_latency_p95 12.45

# HELP kingbus_metrics_read_latency_max read_latency.max
# TYPE kingbus_metrics_read_latency_max gauge
kingbus_metrics_read_latency_max 45.67

# HELP kingbus_metrics_write_tps write_tps
# TYPE kingbus_metrics_write_tps gauge
kingbus_metrics_write_tps 5678.90

# HELP kingbus_metrics_slave_eps_100 slave_eps_100
# TYPE kingbus_metrics_slave_eps_100 gauge
kingbus_metrics_slave_eps_100 1000.00
```

---

## 8. 指标采集点

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '12px', 'fontFamily': 'Arial', 'primaryColor': '#e3f2fd', 'primaryTextColor': '#000000', 'primaryBorderColor': '#1976d2', 'lineColor': '#1976d2', 'background': '#ffffff', 'clusterBkg': '#f5f5f5', 'clusterBorder': '#999999', 'edgeLabelBackground': '#ffffff'}}}%%
graph TB
    subgraph "指标采集位置"
        subgraph "Syncer 采集点"
            SYN["binlog_syncer.go:160<br/>syncerEps.Mark(1)"]
            SYN2["binlog_syncer.go:161<br/>proposeChannelSize.Update()"]
        end
        
        subgraph "Apply 采集点"
            APP["apply.go<br/>applyEps.Mark(1)"]
        end
        
        subgraph "Storage 采集点"
            READ["disk_storage.go:145-146<br/>readLatency.Update()<br/>"]
            READ2["disk_storage.go:175-176<br/>readTps.Mark()<br/>readThroughput.Mark()"]
            WRITE["disk_storage.go:339-341<br/>writeTps.Mark()<br/>writeThroughput.Mark()<br/>writeLatency.Update()"]
        end
        
        subgraph "Slave 采集点"
            SLAVE["command.go:558-559<br/>slaveEps.Mark(1)<br/>slaveThroughput.Mark()"]
        end
    end
    
    style SYN fill:#e3f2fd,stroke:#1976d2,stroke-width:2px
    style READ fill:#e8f5e9,stroke:#388e3c,stroke-width:2px
    style WRITE fill:#fff3e0,stroke:#f57c00,stroke-width:2px
    style SLAVE fill:#fce4ec,stroke:#c2185b,stroke-width:2px
```

---

## 9. 配置参数

| **参数** | **位置** | **默认值** | **说明** |
|---------|---------|-----------|---------|
| `MetricsAddr` | `config.yaml` | `:9596` | Prometheus 端口 |
| `FlushInterval` | `prometheus.go:36` | `1s` | 指标同步间隔 |
| `namespace` | `prometheus.go:44` | `kingbus` | 指标命名空间 |
| `subsystem` | `prometheus.go:45` | `metrics` | 指标子系统 |

---

## 10. 使用方式

### Prometheus 配置

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'kingbus'
    static_configs:
      - targets: ['kingbus-host:9596']
    scrape_interval: 5s
```

### Grafana Dashboard 示例查询

```promql
# Syncer 事件处理速率
kingbus_metrics_syncer_eps

# Storage 读延迟 P95
kingbus_metrics_read_latency_p95

# Storage 写吞吐量
kingbus_metrics_write_throughput

# 所有 Slave 的 EPS
{__name__=~"kingbus_metrics_slave_eps_.*"}
```

---

## 11. 依赖关系

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '12px', 'fontFamily': 'Arial', 'primaryColor': '#e3f2fd', 'primaryTextColor': '#000000', 'primaryBorderColor': '#1976d2', 'lineColor': '#1976d2', 'background': '#ffffff', 'clusterBkg': '#f5f5f5', 'clusterBorder': '#999999', 'edgeLabelBackground': '#ffffff'}}}%%
graph TB
    subgraph "Metrics 依赖"
        PS["PrometheusServer"]
        
        subgraph "外部库"
            GM["rcrowley/go-metrics<br/>指标收集"]
            PC["prometheus/client_golang<br/>Prometheus 客户端"]
            PH["prometheus/promhttp<br/>HTTP Handler"]
        end
        
        subgraph "内部使用"
            SM["server/metrics.go"]
            STM["storage/metrics.go"]
            CMD["mysql/command.go"]
        end
        
        PS --> GM & PC & PH
        SM --> GM
        STM --> GM
        CMD --> GM
    end
    
    style PS fill:#e3f2fd,stroke:#1976d2,stroke-width:2px
    style GM fill:#e8f5e9,stroke:#388e3c,stroke-width:2px
    style PC fill:#fff3e0,stroke:#f57c00,stroke-width:2px
```

---

## 12. 监控告警建议

| **指标** | **告警条件** | **说明** |
|---------|-------------|---------|
| `syncer_eps` | = 0 持续 1 分钟 | Syncer 停止工作 |
| `propose_channel_size` | > 800 | 积压严重，可能需要扩容 |
| `read_latency_p95` | > 100ms | 读性能下降 |
| `write_latency_p95` | > 50ms | 写性能下降 |
| `slave_eps_{id}` | 骤降 | Slave 同步异常 |

