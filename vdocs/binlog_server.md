# Kingbus Binlog Server 参考资料

## 参考文档链接

### 官方文档
- [架构设计文档](../docs/cn/architecture.md) - Kingbus整体架构和核心实现
- [成为Master角色](../docs/cn/become_master.md) - 如何伪装成MySQL Master角色
- [管理API文档](../docs/cn/admin_api.md) - HTTP API管理接口
- [快速开始指南](../docs/en/quick_start.md) - 部署和配置指南
- [Docker部署](../docs/cn/docker_compose.md) - 容器化部署方案

### 核心实现参考
- [command.go](https://github.com/flike/kingbus/blob/master/mysql/command.go) - MySQL协议命令实现
- [Facebook Binlog Server论文](../docs/binlog_server_at_fackbook.pdf) - 设计理论基础

### 架构图片
- [Kingbus架构图](../docs/img/kingbus_arch.png)
- [Kingbus架构图2](../docs/img/kingbus_arch2.png) 
- [存储架构图](../docs/img/kingbus_storage.png)
- [网络拓扑图](../docs/img/kingbus_topology.png)

## 关键特性

### MySQL复制协议兼容
- 完全实现MySQL复制协议
- 支持GTID模式的binlog同步
- 兼容MySQL主从复制机制

### 核心功能
- 伪装成MySQL Master角色
- 向下游MySQL从库发送binlog事件
- 处理MySQL复制协议请求
- 定期发送heartbeat事件保持复制连接

### 管理接口
- 启动/停止binlog server
- 查看服务状态
- 连接的从库管理

## 实现原理

基于docs/cn/become_master.md文档，Binlog Server实现了完整的MySQL Master功能：

### MySQL主从复制命令序列
```sql
SELECT UNIX_TIMESTAMP()
SHOW VARIABLES LIKE 'SERVER_ID'  
SET @master_heartbeat_period=?
SET @master_binlog_checksum= @@global.binlog_checksum
SELECT @master_binlog_checksum
SELECT @@GLOBAL.GTID_MODE
SHOW VARIABLES LIKE 'SERVER_UUID'
SET @slave_uuid= '%s'
COM_REGISTER_SLAVE
COM_BINLOG_DUMP
```

### 核心实现文件
所有MySQL协议命令都在`mysql/command.go`文件中实现，包括：
- COM_QUERY: 处理SQL查询命令
- COM_REGISTER_SLAVE: 注册从库信息  
- COM_BINLOG_DUMP: 处理binlog请求
- COM_BINLOG_DUMP_GTID: 处理基于GTID的binlog请求
