# 部署指南

> **文档版本**：v1.1  
> **更新日期**：2025-11-07  
> **对应代码版本**：data_analysis v1.1 (branch: feature-one)

本文档提供data_analysis模块的部署指南，包括环境要求、部署步骤、监控和日志配置，以及高可用配置方案。

**修订说明**：
- v1.1 (2025-11-07): 更新以匹配实际部署需求
  - 更正配置中的字段命名（使用camelCase）
  - 更新Kafka配置示例与实际代码一致
  - 补充实际的依赖包列表
  - 更新监控和日志配置为实际实现
  - 完善容器化和Kubernetes部署示例
  - 添加Python 3.9+支持说明

## 目录

1. [环境要求](#环境要求)
2. [部署步骤](#部署步骤)
3. [监控和日志](#监控和日志)
4. [高可用配置](#高可用配置)

## 环境要求

### 软件要求

- **Python**: 3.9 或更高版本（推荐 3.10 或 3.11）
- **Kafka**: 2.2 或更高版本集群（推荐 2.8+）
- **操作系统**: Linux/Unix（推荐），Windows 也可支持

### 依赖包要求

核心依赖包：
```txt
kafka-python>=2.0.2      # 同步Kafka客户端
aiokafka>=0.7.2          # 异步Kafka客户端
```

可选依赖（用于开发和测试）：
```txt
pytest>=7.0.0            # 单元测试
pytest-asyncio>=0.18.0   # 异步测试支持
pytest-cov>=3.0.0        # 代码覆盖率
```

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

# 或手动安装核心依赖
pip install kafka-python>=2.0.2 aiokafka>=0.7.2

# 开发环境额外安装
pip install pytest pytest-asyncio pytest-cov
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

**重要配置项说明**：

#### Kafka配置

推荐使用嵌套格式，为消费者和生产者分别配置参数：

```python
KAFKA_CONFIG = {
    'consumer': {
        'bootstrap_servers': ['kafka1:9092', 'kafka2:9092', 'kafka3:9092'],
        'group_id': 'data_analysis_group',
        'auto_offset_reset': 'latest',
        'enable_auto_commit': True,
        'max_poll_records': 500,
        'session_timeout_ms': 30000,
        'request_timeout_ms': 40000,
        'heartbeat_interval_ms': 3000,
    },
    'producer': {
        'bootstrap_servers': ['kafka1:9092', 'kafka2:9092', 'kafka3:9092'],
        'acks': 'all',  # 生产环境建议使用'all'保证数据可靠性
        'retries': 3,
        'max_in_flight_requests_per_connection': 5,
        'compression_type': 'gzip',  # 启用压缩节省网络带宽
        'linger_ms': 10,  # 批量发送延迟，提高吞吐量
        'batch_size': 16384,  # 批量大小
    }
}
```

**配置说明**：

- `bootstrap_servers`: 配置多个Kafka broker地址以支持高可用
- `acks='all'`: 生产环境建议使用，等待所有副本确认，确保数据不丢失
- `compression_type='gzip'`: 启用压缩可以节省网络带宽和存储空间
- `max_poll_records=500`: 根据数据量和处理能力调整，避免消费超时

#### 窗口和补全配置

```python
# Topic配置中设置窗口大小
TOPIC_DETAIL = {
    'REAL-STATION-DATA': {
        'window_size': 5,  # 根据业务需求设置合适的窗口大小
        # ...
    },
    # ...
}

# 在服务初始化时设置补全策略
# 'zero': 补零（默认，适合累计值）
# 'linear': 线性插值（适合连续变化的数据）
# 'forward': 前向填充（适合状态数据）
# 'missing': 标记为缺失（适合需要明确区分缺失数据的场景）
```

#### 性能相关配置

```python
# 同步服务：线程池大小
max_workers = 32  # 根据场站数量和CPU核心数调整

# 数据过期时间
data_expire_seconds = 600  # 10分钟，根据数据特点调整

# 异步服务：并发任务数
# 由asyncio自动管理，但需注意系统资源限制
```

### 3. 服务启动脚本

#### 同步服务启动示例

创建 `sync_main.py`：

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
同步服务启动脚本
"""
from d_a import DataAnalysisService
from d_a.config import KAFKA_CONFIG
import logging

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(name)s %(message)s'
)

def my_callback(station_id, module_input):
    """处理数据的回调函数"""
    logging.info(f"处理场站 {station_id} 的数据")
    # 这里添加你的业务逻辑
    return module_input  # 返回结果将自动上传到Kafka

if __name__ == '__main__':
    service = DataAnalysisService(
        module_name='load_prediction',  # 指定模块名称
        kafka_config=KAFKA_CONFIG,
        data_expire_seconds=600
    )
    
    try:
        service.start(callback=my_callback, background=False)
    except KeyboardInterrupt:
        logging.info("收到停止信号，正在关闭服务...")
        service.stop()
```

#### 异步服务启动示例

创建 `async_main.py`：

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
异步服务启动脚本
"""
import asyncio
from d_a import AsyncDataAnalysisService
from d_a.config import KAFKA_CONFIG
import logging

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(name)s %(message)s'
)

async def my_callback(station_id, module_input):
    """异步处理数据的回调函数"""
    logging.info(f"处理场站 {station_id} 的数据")
    # 这里添加你的异步业务逻辑
    await asyncio.sleep(0.01)  # 模拟异步操作
    return module_input  # 返回结果将自动上传到Kafka

async def main():
    service = AsyncDataAnalysisService(
        module_name='load_prediction',  # 指定模块名称
        kafka_config=KAFKA_CONFIG,
        data_expire_seconds=600
    )
    
    try:
        await service.start(callback=my_callback)
        # 保持运行
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        logging.info("收到停止信号，正在关闭服务...")
        await service.stop()

if __name__ == '__main__':
    asyncio.run(main())
```

#### Linux系统服务启动脚本

创建 `/opt/data_analysis/start.sh`：

```bash
#!/bin/bash
# 启动脚本

# 激活虚拟环境
source /opt/data_analysis/venv/bin/activate

# 设置环境变量
export PYTHONPATH=/opt/data_analysis
export DATA_ANALYSIS_LOG=/var/log/data_analysis/app.log

# 启动服务（选择同步或异步）
cd /opt/data_analysis
python sync_main.py

# 或使用异步服务
# python async_main.py
```

使脚本可执行：
```bash
chmod +x /opt/data_analysis/start.sh
```

### 4. 系统服务配置（Linux）

创建`/etc/systemd/system/data_analysis.service`：

```ini
[Unit]
Description=Data Analysis Service
After=network.target

[Service]
Type=simple
User=analysis
Group=analysis
WorkingDirectory=/opt/data_analysis
Environment=PYTHONPATH=/opt/data_analysis
Environment=DATA_ANALYSIS_LOG=/var/log/data_analysis/app.log
ExecStart=/opt/data_analysis/venv/bin/python /opt/data_analysis/sync_main.py
Restart=always
RestartSec=10
StandardOutput=append:/var/log/data_analysis/stdout.log
StandardError=append:/var/log/data_analysis/stderr.log

[Install]
WantedBy=multi-user.target
```

然后启用并启动服务：

```bash
# 创建日志目录
sudo mkdir -p /var/log/data_analysis
sudo chown analysis:analysis /var/log/data_analysis

# 启用并启动服务
sudo systemctl daemon-reload
sudo systemctl enable data_analysis
sudo systemctl start data_analysis

# 查看服务状态
sudo systemctl status data_analysis

# 查看日志
sudo journalctl -u data_analysis -f
```

### 5. 容器化部署

创建`Dockerfile`：

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# 安装系统依赖（如需要）
# RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt .

# 安装Python依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY d_a/ ./d_a/
COPY sync_main.py .
COPY async_main.py .

# 创建非root用户
RUN useradd -m -u 1000 analysis && \
    mkdir -p /var/log/data_analysis && \
    chown -R analysis:analysis /app /var/log/data_analysis

USER analysis

# 设置环境变量
ENV PYTHONPATH=/app
ENV DATA_ANALYSIS_LOG=/var/log/data_analysis/app.log

# 默认启动同步服务
CMD ["python", "sync_main.py"]
```

创建 `requirements.txt`：

```txt
kafka-python>=2.0.2
aiokafka>=0.7.2
```

构建和运行：

```bash
# 构建镜像
docker build -t data_analysis:v1.1 .

# 运行容器（同步服务）
docker run -d \
  --name data_analysis_sync \
  -v /path/to/config.py:/app/d_a/config.py \
  -v /path/to/logs:/var/log/data_analysis \
  data_analysis:v1.1

# 运行容器（异步服务）
docker run -d \
  --name data_analysis_async \
  -v /path/to/config.py:/app/d_a/config.py \
  -v /path/to/logs:/var/log/data_analysis \
  data_analysis:v1.1 \
  python async_main.py

# 查看日志
docker logs -f data_analysis_sync
```

### 6. Kubernetes部署

创建`deployment.yaml`：

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: data-analysis
  namespace: production
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
        image: data_analysis:v1.1
        env:
        - name: PYTHONPATH
          value: "/app"
        - name: DATA_ANALYSIS_LOG
          value: "/var/log/data_analysis/app.log"
        volumeMounts:
        - name: config-volume
          mountPath: /app/d_a/config.py
          subPath: config.py
        - name: log-volume
          mountPath: /var/log/data_analysis
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "2000m"
        livenessProbe:
          exec:
            command:
            - python
            - -c
            - "import sys; sys.exit(0)"
          initialDelaySeconds: 30
          periodSeconds: 30
      volumes:
      - name: config-volume
        configMap:
          name: data-analysis-config
      - name: log-volume
        emptyDir: {}
```

创建 `service.yaml`（如果需要对外暴露）：

```yaml
apiVersion: v1
kind: Service
metadata:
  name: data-analysis-service
  namespace: production
spec:
  selector:
    app: data-analysis
  ports:
  - protocol: TCP
    port: 8080
    targetPort: 8080
  type: ClusterIP
```

创建 `configmap.yaml`：

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: data-analysis-config
  namespace: production
data:
  config.py: |
    # -*- coding: utf-8 -*-
    """
    配置文件
    """
    
    # Kafka配置（嵌套格式，推荐）
    KAFKA_CONFIG = {
        'consumer': {
            'bootstrap_servers': ['kafka-broker-1:9092', 'kafka-broker-2:9092', 'kafka-broker-3:9092'],
            'group_id': 'data_analysis_prod',
            'auto_offset_reset': 'latest',
            'enable_auto_commit': True,
            'max_poll_records': 500,
            'session_timeout_ms': 30000,
            'request_timeout_ms': 40000,
            'heartbeat_interval_ms': 3000,
        },
        'producer': {
            'bootstrap_servers': ['kafka-broker-1:9092', 'kafka-broker-2:9092', 'kafka-broker-3:9092'],
            'acks': 'all',
            'retries': 3,
            'max_in_flight_requests_per_connection': 5,
            'compression_type': 'gzip',
            'linger_ms': 10,
            'batch_size': 16384,
        }
    }
    
    # Topic配置（注意使用camelCase字段名）
    TOPIC_DETAIL = {
        'SCHEDULE-STATION-PARAM': {
            'fields': ['stationId', 'stationTemp', 'lat', 'lng', ...],
            'frequency': '新建站或配置更改时',
            'modules': ['load_prediction', 'operation_optimization'],
            'window_size': 1
        },
        # 其他topic配置...
    }
    
    # 模块依赖配置
    MODULE_DEPENDENCIES = {
        'operation_optimization': ['load_prediction'],
        'electricity_price': ['pv_prediction', 'evaluation_model', 'SOH_model'],
        # 其他依赖配置...
    }
    
    # 模块输出topic映射
    MODULE_OUTPUT_TOPICS = {
        'load_prediction': 'MODULE-OUTPUT-LOAD-PREDICTION',
        'operation_optimization': 'MODULE-OUTPUT-OPERATION-OPTIMIZATION',
        # 其他模块...
    }

```

部署应用：

```bash
# 创建命名空间
kubectl create namespace production

# 应用配置
kubectl apply -f configmap.yaml
kubectl apply -f deployment.yaml
kubectl apply -f service.yaml

# 查看部署状态
kubectl get pods -n production
kubectl get svc -n production

# 查看日志
kubectl logs -f deployment/data-analysis -n production

# 扩容
kubectl scale deployment/data-analysis --replicas=5 -n production
```

## 监控和日志

### 日志配置

data_analysis模块使用Python标准logging模块，日志配置在代码中设置。

**日志文件位置**：
- 主日志文件：`data_analysis.log`（默认在当前目录，可通过环境变量 `DATA_ANALYSIS_LOG` 指定）
- 格式：`[时间戳] 日志级别 模块名 消息内容`
- 编码：UTF-8
- 轮转：建议使用 `RotatingFileHandler`，每个文件最大10MB，保留5个备份

**日志级别**：
- `INFO`：正常操作信息（服务启动、数据处理、配置热更新等）
- `ERROR`：错误信息（Kafka连接失败、数据解析错误、回调异常等）
- `DEBUG`：调试信息（详细的数据流、中间状态等）

**日志配置示例**：

```python
import logging
import os
from logging.handlers import RotatingFileHandler

# 日志文件路径（可通过环境变量指定）
LOG_FILE = os.getenv("DATA_ANALYSIS_LOG", "data_analysis.log")
LOG_FORMAT = "[%(asctime)s] %(levelname)s %(name)s %(message)s"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# 配置日志
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(),  # 控制台输出
        RotatingFileHandler(
            LOG_FILE,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding="utf-8"
        )
    ]
)
```

**环境变量配置**：
```bash
# Linux
export DATA_ANALYSIS_LOG=/var/log/data_analysis/app.log
export LOG_LEVEL=INFO

# Windows PowerShell
$env:DATA_ANALYSIS_LOG="C:\logs\data_analysis\app.log"
$env:LOG_LEVEL="INFO"
```

### 监控指标

建议监控以下关键指标：

1. **服务状态指标**：
   - 服务进程是否正常运行
   - 主线程状态（使用 `get_service_status()` API）
   - Kafka消费者/生产者连接状态
   - 活跃场站数量和任务状态（使用 `get_station_status()` API）

2. **性能指标**：
   - Kafka消费延迟（lag）
   - 数据处理速率（条/秒）
   - 回调处理耗时
   - 内存使用率
   - CPU使用率

3. **业务指标**：
   - 各场站数据接收量
   - 错误率（按错误类型分类）
   - 数据窗口缓存大小
   - 模块依赖聚合成功率

**健康检查API示例**：

```python
def health_check():
    """
    健康检查函数，可集成到监控系统
    """
    from d_a import DataAnalysisService
    
    service = DataAnalysisService()  # 获取服务实例
    
    # 获取服务状态
    service_status = service.get_service_status()
    station_status = service.get_station_status()
    
    health = {
        "status": "healthy" if service_status.get('main_thread_alive') else "unhealthy",
        "timestamp": time.time(),
        "service": {
            "main_thread": service_status.get('main_thread_alive', False),
            "consumer_alive": service_status.get('consumer_alive', False),
            "producer_alive": service_status.get('producer_alive', False),
            "station_count": service_status.get('station_count', 0)
        },
        "stations": {
            sid: {"running": info.get('running', False)}
            for sid, info in station_status.items()
        }
    }
    
    return health
```

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

