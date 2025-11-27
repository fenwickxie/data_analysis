# 快速开始 - 打包和使用 data_analysis

## 📦 打包 wheel 文件

### 1. 准备环境

```bash
pip install build wheel setuptools
```

### 2. 构建包

```bash
cd /path/to/data_analysis
python -m build
```

生成的文件：
```
dist/
├── data_analysis-2.0.0-py3-none-any.whl  ← 这是需要的文件
└── data_analysis-2.0.0.tar.gz
```

---

## 🚀 在模型项目中使用

### 步骤 1: 安装 wheel 包

```bash
pip install /path/to/data_analysis-2.0.0-py3-none-any.whl
```

### 步骤 2: 创建项目文件

```bash
# 创建项目目录
mkdir my_model_project
cd my_model_project

# 复制配置文件和入口文件模板
cp /path/to/data_analysis/config.yaml.example config.yaml
cp /path/to/data_analysis/main.py.example main.py
```

你的项目结构：
```
my_model_project/
├── config.yaml      # 配置文件
├── main.py          # 服务入口
└── my_model.py      # 你的模型实现（可选）
```

### 步骤 3: 配置 config.yaml

编辑 `config.yaml`，**必须修改**以下两项：

```yaml
# 1. 设置你的模块名称（重要！）
module_name: "electricity_price"  # 改为你的模块名

# 2. 配置 Kafka 服务器
kafka:
  bootstrap_servers:
    - 'your_kafka_server:9092'  # 改为你的 Kafka 地址
```

可用的模块名称：
- `load_prediction` - 负荷预测
- `electricity_price` - 电价策略
- `pv_prediction` - 光伏预测
- `thermal_management` - 热管理
- `station_guidance` - 站点引导
- `evaluation_model` - 评估模型
- `SOH_model` - SOH模型
- `operation_optimization` - 运营优化
- `customer_mining` - 客户挖掘

### 步骤 4: 实现模型逻辑

编辑 `main.py`，找到 `my_model_predict` 函数，替换为你的模型：

```python
async def my_model_predict(module_input):
    """
    你的模型推理逻辑
    """
    # 导入你的模型
    from my_model import MyModel
    
    # 模型推理
    model = MyModel()
    result = await model.predict(module_input)
    
    return result
```

### 步骤 5: 运行服务

```bash
python main.py
```

---

## 📝 完整示例

### 示例：电价预测模型项目

```bash
# 1. 创建项目
mkdir electricity_price_model
cd electricity_price_model

# 2. 安装包
pip install /path/to/data_analysis-2.0.0-py3-none-any.whl

# 3. 创建文件
cp /path/to/data_analysis/config.yaml.example config.yaml
cp /path/to/data_analysis/main.py.example main.py

# 4. 编辑 config.yaml
cat > config.yaml << 'EOF'
# 模块名称
module_name: "electricity_price"

# Kafka配置
kafka:
  bootstrap_servers:
    - 'localhost:9092'
  consumer:
    group_id: 'electricity-price-model'
    auto_offset_reset: 'latest'
    multi_consumer_mode: true
    max_poll_records: 10
    enable_auto_commit: false

# 其他配置...（从 config.yaml.example 复制）
EOF

# 5. 创建模型文件
cat > my_model.py << 'EOF'
import asyncio

class ElectricityPriceModel:
    async def predict(self, input_data):
        # 你的模型推理逻辑
        await asyncio.sleep(0.01)  # 模拟推理
        return {
            "price": 0.5,
            "strategy": "peak_valley"
        }
EOF

# 6. 修改 main.py 的 my_model_predict 函数
# （在编辑器中修改）

# 7. 运行
python main.py
```

---

## 🔧 常见问题

### Q: 找不到 config.yaml
**A:** 确保 config.yaml 在运行 main.py 的当前目录下

### Q: ModuleNotFoundError: No module named 'd_a'
**A:** 确认已安装 data_analysis 包：`pip show data_analysis`

### Q: 未配置模块的输出topic
**A:** 检查 config.yaml 中的 module_name 是否正确，必须是支持的模块名称之一

### Q: Kafka 连接失败
**A:** 检查 config.yaml 中的 bootstrap_servers 地址是否正确

---

## 📚 更多信息

- **完整配置说明**：[docs/CONFIG.md](../docs/CONFIG.md)
- **详细打包指南**：[docs/PACKAGING.md](../docs/PACKAGING.md)
- **API 文档**：[docs/API.md](../docs/API.md)
- **部署指南**：[docs/DEPLOYMENT.md](../docs/DEPLOYMENT.md)

---

## 💡 提示

1. **开发模式**：使用 `pip install -e /path/to/data_analysis` 可以实时看到修改
2. **版本管理**：在 requirements.txt 中固定版本：`data_analysis==2.0.0`
3. **环境隔离**：为每个模型项目使用独立的虚拟环境
4. **配置安全**：不要将包含敏感信息的 config.yaml 提交到版本控制
