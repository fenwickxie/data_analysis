# 部署指南

本文档提供data_analysis模块的部署指南，包括环境要求、部署步骤、监控和日志配置，以及高可用配置方案。

## 目录

1. [环境要求](#环境要求)
2. [部署步骤](#部署步骤)
3. [监控和日志](#监控和日志)
4. [高可用配置](#高可用配置)

## 环境要求

### 软件要求

- **Python**: 3.7或更高版本
- **Kafka**: 2.2或更高版本集群
- **操作系统**: Linux/Unix（推荐），Windows也可支持

### 硬件要求

- **CPU**: 根据场站数量和数据处理需求，建议至少4核
- **内存**: 建议8GB以上，根据数据缓存需求可适当增加
- **存储**: 根据数据保留策略配置，建议SSD提高IO性能

### 网络要求

- Kafka集群与data_analysis服务之间需要稳定网络连接
- 如果部署在云环境，确保安全组配置正确，允许相关端口通信

## 部署步骤

### 1. 环境准备

```bash
# 创建部署目录
sudo mkdir -p /opt/data_analysis
sudo chown -R $USER:$USER /opt/data_analysis

# 克隆代码
cd /opt/data_analysis
git clone [仓库地址] .

# 创建虚拟环境（推荐）
python -m venv venv
source venv/bin/activate  # Linux/Mac
# 或 venv\Scripts\activate  # Windows

# 安装依赖
pip install -r requirements.txt
pip install gunicorn  # 用于进程管理
```

### 2. 配置文件准备

```bash
# 创建配置目录
mkdir -p /opt/data_analysis/config

# 复制并修改配置文件
cp data_analysis/config.py /opt/data_analysis/config/
# 编辑配置文件，修改Kafka连接等参数
vim /opt/data_analysis/config/config.py
```

### 3. 服务启动脚本

创建`/opt/data_analysis/start.sh`（Linux）或`start.bat`（Windows）：

```bash
#!/bin/bash
# 启动脚本

# 激活虚拟环境
source /opt/data_analysis/venv/bin/activate

# 设置环境变量
export PYTHONPATH=/opt/data_analysis
export DATA_ANALYSIS_CONFIG=/opt/data_analysis/config/config.py

# 启动服务
# 同步服务示例
gunicorn -w 4 -b 0.0.0.0:8000 data_analysis.main:sync_app

# 异步服务示例
# gunicorn -w 4 -k gunicorn.workers.gthread -b 0.0.0.0:8000 data_analysis.async_main:async_app
```

### 4. 系统服务配置（Linux）

创建`/etc/systemd/system/data_analysis.service`：

```ini
[Unit]
Description=Data Analysis Service
After=network.target

[Service]
Type=forking
User=analysis
Group=analysis
WorkingDirectory=/opt/data_analysis
Environment=PYTHONPATH=/opt/data_analysis
Environment=DATA_ANALYSIS_CONFIG=/opt/data_analysis/config/config.py
ExecStart=/opt/data_analysis/start.sh
ExecReload=/bin/kill -s HUP $MAINPID
KillMode=mixed
TimeoutStopSec=5
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

然后启用并启动服务：

```bash
sudo systemctl daemon-reload
sudo systemctl enable data_analysis
sudo systemctl start data_analysis
```

### 5. 容器化部署

创建`Dockerfile`：

```dockerfile
FROM python:3.9-slim

WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y gcc

# 复制依赖文件
COPY requirements.txt .

# 安装Python依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY . .

# 创建非root用户
RUN useradd -m -u 1000 analysis && chown -R analysis:analysis /app
USER analysis

# 设置入口点
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:8000", "data_analysis.main:sync_app"]
```

构建和运行：

```bash
# 构建镜像
docker build -t data_analysis .

# 运行容器
docker run -d --name data_analysis   -p 8000:8000   -v /opt/data_analysis/config:/app/config   data_analysis
```

### 6. Kubernetes部署

创建`deployment.yaml`：

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: data-analysis
spec:
  replicas: 3
  selector:
    matchLabels:
      app: data-analysis
  template:
    metadata:
      labels:
        app: data-analysis
    spec:
      containers:
      - name: data-analysis
        image: data_analysis:latest
        ports:
        - containerPort: 8000
        env:
        - name: PYTHONPATH
          value: "/app"
        - name: DATA_ANALYSIS_CONFIG
          value: "/app/config/config.py"
        volumeMounts:
        - name: config-volume
          mountPath: /app/config
      volumes:
      - name: config-volume
        configMap:
          name: data-analysis-config
```

创建`configmap.yaml`：

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: data-analysis-config
data:
  config.py: |
    # 配置文件内容
    KAFKA_CONFIG = {
        'bootstrap_servers': ['kafka-service:9092'],
        'group_id': 'data_analysis_group',
        'auto_offset_reset': 'latest',
        'enable_auto_commit': True,
    }
```

## 监控和日志

### 日志配置

修改`data_analysis/__init__.py`中的日志配置：

```python
import logging
import os

# 日志支持多级别和文件输出
LOG_FORMAT = "[%(asctime)s] %(levelname)s %(name)s %(message)s"
LOG_FILE = os.getenv("DATA_ANALYSIS_LOG", "data_analysis.log")

logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.handlers.RotatingFileHandler(
            LOG_FILE, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8"
        ),
    ],
)
```

### 监控指标

1. **服务状态指标**：
   - 服务是否正常运行
   - 主线程状态
   - Kafka连接状态
   - 场站任务数量和状态

2. **性能指标**：
   - 数据处理速率
   - 内存使用率
   - CPU使用率
   - Kafka消费/生产延迟

3. **业务指标**：
   - 各模块处理数据量
   - 错误率
   - 回调处理时间

### Prometheus监控集成

添加Prometheus客户端：

```python
# 在data_analysis/__init__.py中添加
from prometheus_client import start_http_server, Counter, Gauge, Histogram

# 定义指标
DATA_PROCESSED = Counter('data_analysis_processed_total', 'Total data processed')
ERROR_COUNT = Counter('data_analysis_errors_total', 'Total errors', ['error_type'])
MEMORY_USAGE = Gauge('data_analysis_memory_usage', 'Memory usage in MB')
STATION_COUNT = Gauge('data_analysis_station_count', 'Number of active stations')
PROCESSING_TIME = Histogram('data_analysis_processing_seconds', 'Processing time')

# 在服务启动时启动metrics服务器
start_http_server(8001)  # 在8001端口暴露metrics
```

### 日志分析

使用ELK（Elasticsearch, Logstash, Kibana）或类似方案进行日志分析：

1. **Filebeat配置**：
```yaml
filebeat.inputs:
- type: log
  enabled: true
  paths:
    - /opt/data_analysis/logs/*.log
  fields:
    app: data_analysis
  fields_under_root: true

output.elasticsearch:
  hosts: ["elasticsearch:9200"]
```

2. **常用查询**：
   - 错误日志查询：`level:ERROR`
   - 特定场站日志：`station_id:station123`
   - 性能分析：`processing_time > 1s`

## 高可用配置

### 多实例部署

1. **负载均衡**：
   使用Nginx或HAProxy进行负载均衡：

```nginx
upstream data_analysis {
    server 192.168.1.10:8000;
    server 192.168.1.11:8000;
    server 192.168.1.12:8000;
}

server {
    listen 80;
    location / {
        proxy_pass http://data_analysis;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

2. **共享配置**：
   - 使用共享存储或配置服务器同步配置文件
   - 考虑使用Consul或etcd进行配置管理

3. **Kafka高可用**：
   - 确保Kafka集群配置了多个broker
   - 设置适当的副本因子和分区数
   - 启用Kafka的自动故障转移机制

### 故障转移机制

1. **健康检查**：
   实现健康检查端点：

```python
# 在data_analysis/main.py中添加
@app.route('/health')
def health():
    return jsonify({
        "status": "healthy",
        "timestamp": time.time(),
        "version": "1.0.0"
    })
```

2. **自动重启**：
   - 使用systemd的自动重启功能
   - 配置适当的重启策略：

```ini
[Service]
Restart=always
RestartSec=10
```

3. **数据备份**：
   - 定期备份配置文件
   - 考虑对关键数据进行持久化存储

### 分布式部署方案

对于大规模场站，可采用分布式部署：

1. **按场站分组**：
   - 将场站按区域或类型分组
   - 每组部署独立的服务实例

2. **数据分区**：
   - 根据场站ID进行数据分区
   - 确保同一场站的数据在同一实例处理

3. **依赖服务**：
   - 为依赖服务（如数据库）配置高可用方案
   - 使用连接池管理连接

### 容灾方案

1. **异地容灾**：
   - 在不同地理位置部署备用节点
   - 配置数据同步机制

2. **故障恢复**：
   - 制定详细的故障恢复流程
   - 定期进行故障演练

3. **数据一致性**：
   - 确保故障切换时数据一致性
   - 使用适当的同步机制
