# EPUB LLM Extraction MVP 使用说明

本项目实现了一个面向 EPUB 小说的最小可行抽取流程：

- 按章节读取 EPUB
- 依据公共 `schemas/` 中的 schema 定义抽取目标
- 按 `EPUB × schema` 任务粒度运行
- 为每个 EPUB 创建独立 `workspace/` 工作区
- 使用 `prompts/` 模板构造提示词
- 支持流式缓冲、最小结构校验、增量替换合并、状态落盘与恢复
- 已支持 OpenAI-compatible Chat Completions 流式 API 接入

当前版本已经完成 MVP 骨架、基础测试、本地可运行入口，以及真实 API 客户端接入。

---

## 1. 目录说明

当前关键目录如下：

- `src/`：主程序源码
- `schemas/`：公共 schema 文件
- `prompts/`：提示词模板
- `workspace/`：运行时工作区，按 EPUB 隔离
- `tests/`：单元测试
- `config.yaml`：运行配置

核心模块：

- `src/app.py`：程序入口
- `src/task_runner.py`：单个 `EPUB × schema` 任务执行器
- `src/epub_reader.py`：EPUB 章节提取
- `src/schema_loader.py`：schema 装载与字段提取
- `src/schema_validator.py`：最小结构校验
- `src/yaml_store.py`：输出 YAML 初始化、读取与增量合并
- `src/progress_store.py`：任务进度文件持久化
- `src/workspace_manager.py`：工作区创建与路径管理
- `src/llm_client.py`：流式模型客户端实现

---

## 2. 运行环境

建议环境：

- Python 3.10+

当前代码使用到的 Python 包：

- `PyYAML`
- `lxml`

如果本地未安装，可执行：

```bash
pip install pyyaml lxml
```

当前 API 接入基于 Python 标准库 `urllib`，因此不强制依赖额外 HTTP SDK。

---

## 3. 当前实现状态

### 已实现

- EPUB 章节顺序抽取
- 工作区初始化
- schema 根节点与字段路径提取
- 提示词模板加载与渲染
- 流式缓冲文件写入
- YAML 解析与最小结构验证
- 节点级整节点替换合并
- 章节级进度文件落盘
- 模板顺序尝试
- 有界重试骨架
- OpenAI-compatible 流式 API 接入
- 单元测试

### 尚未实现 / 仍待增强

- LangChain/LangGraph 生产级接入
- 更强 schema 注释保留与注释驱动校验
- 更完整的错误分类与观测能力
- 更丰富的多提供方适配

如果 `config.yaml` 中 `model.api_key` 为空，系统会退回到 `src/llm_client.py` 中的占位客户端，仅返回空增量 YAML，用于本地打通流程。

---

## 4. 配置文件说明

配置文件是 `config.yaml`。

示例：

```yaml
input_epubs:
  - input/novel.epub
schema_paths:
  - schemas/characters.yaml
  - schemas/world.yaml
prompt_templates:
  - prompts/base.md
  - prompts/retry_format.md
  - prompts/retry_schema.md
workspace_root: workspace
concurrency:
  enable_parallel_tasks: true
  task_unit: epub_schema
  max_workers: 4
model:
  provider: openai
  name: gpt-4.1
  streaming: true
  base_url: https://api.openai.com/v1
  api_key: <KEY>
runtime:
  resume: true
  retry_count: 3
  retry_backoff_seconds: 3
progress:
  emit_console_progress: true
```

### 字段说明

#### `input_epubs`
要处理的 EPUB 文件列表。当前默认是空列表，需要手动填写。

#### `schema_paths`
要启用的 schema 列表。默认包含：

- `schemas/characters.yaml`
- `schemas/world.yaml`

#### `prompt_templates`
提示词模板顺序。当前默认顺序：

1. `prompts/base.md`
2. `prompts/retry_format.md`
3. `prompts/retry_schema.md`

#### `workspace_root`
工作区根目录，默认是 `workspace`。

#### `concurrency.enable_parallel_tasks`
是否允许并发执行多个 `EPUB × schema` 任务。

#### `concurrency.max_workers`
最大并发 worker 数。

#### `model.provider`
模型提供方标识。当前版本主要按 OpenAI-compatible 协议处理，推荐填 `openai`。

#### `model.name`
要调用的模型名称，例如 `gpt-4.1`。

#### `model.streaming`
是否启用流式输出。当前任务执行器按流式缓冲方式组织流程。

#### `model.base_url`
模型服务地址，当前会拼接为 `{{base_url}}/chat/completions` 发起请求。

#### `model.api_key`
模型 API Key。若留空，则自动退回到占位客户端，不会请求真实在线模型。

#### `runtime.resume`
是否启用断点恢复。启用后会复用已有进度文件。

#### `runtime.retry_count`
单章节最大重试次数。

#### `runtime.retry_backoff_seconds`
章节级重试之间的等待秒数。

#### `progress.emit_console_progress`
是否输出控制台进度日志。

---

## 5. 输入准备

建议目录结构如下：

```text
input/
  novel.epub
  other_novel.epub
```

然后在 `config.yaml` 中填写：

```yaml
input_epubs:
  - input/novel.epub
  - input/other_novel.epub
```

如果 `input/` 目录还不存在，可以自行创建。

---

## 6. 运行方式

在项目根目录执行：

```bash
python src/app.py
```

### 运行结果说明

#### 情况 1：未配置 `input_epubs`
程序会输出：

```text
No tasks found. Please check config.yaml input_epubs and schema_paths.
```

这属于正常行为，表示配置里没有待处理任务。

#### 情况 2：已配置 EPUB，但 `model.api_key` 为空
程序会使用占位客户端打通处理流程，输出通常是空增量结果，主要用于验证目录、状态文件和流程逻辑。

#### 情况 3：已配置 EPUB，且 `model.api_key` 有效
程序会：

1. 为每个 EPUB 创建独立工作区
2. 为每个 schema 建立一个任务
3. 顺序处理章节
4. 通过 OpenAI-compatible 流式接口拉取模型输出
5. 生成输出 YAML、进度 YAML、流式临时文件与日志文件

控制台会打印类似进度信息：

```text
[novel][characters] 1/120 template=base status=running retries=0 replaced=0 appended=0
```

---

## 7. 工作区结构说明

程序运行后，会在 `workspace/` 下生成按 EPUB 隔离的目录，例如：

```text
workspace/
  novel/
    source/
      novel.epub
    output/
      characters.yaml
      world.yaml
    state/
      characters.progress.yaml
      world.progress.yaml
    temp/
      characters.stream.txt
      world.stream.txt
    logs/
      run.log
```

### 各目录用途

#### `source/`
保存输入 EPUB 的副本。

#### `output/`
保存每个 schema 对应的正式输出 YAML。

#### `state/`
保存章节进度、最近错误、重试状态、模板状态、合并统计等。

#### `temp/`
保存当前章节的流式输出缓冲。

#### `logs/`
预留运行日志目录。

---

## 8. 输出语义说明

当前采用的是**节点级增量替换**，不是字段级 patch。

### `characters.yaml`
- 根节点：`actors`
- 匹配键：`name`

### `world.yaml`
- 根节点：`worldinfo`
- 匹配键：`name`

### 合并规则

对于每个本章返回的节点：

- 如果已存在同名节点：整节点替换
- 如果不存在同名节点：追加到列表末尾

### 非法输出示例

以下情况会被视为非法：

- 缺少根节点
- 根节点不是列表
- 列表元素不是对象
- 节点缺少 `name`
- 输出了 schema 中不存在的明显非法字段

---

## 9. 恢复机制说明

当 `config.yaml` 中的 `runtime.resume: true` 时：

- 若存在进度文件，则从 `last_completed_chapter_index` 的下一章继续
- 若存在未完成的流式缓冲文件，则会先清理再重新处理当前章节
- 若正式输出已存在，会继续在已有 YAML 基础上做整节点替换/追加

这使得长任务中断后可以继续运行，而不必每次从头开始。

---

## 10. API 接入说明

当前已经支持 OpenAI-compatible Chat Completions 流式接口。

实现位置：

- `src/llm_client.py`
- `src/task_runner.py`

### 当前接入方式

当 `TaskRunner` 初始化且未手动注入模型客户端时，会调用 `src/llm_client.py` 中的客户端构造逻辑：

- 若 `model.api_key` 非空，使用真实 API 客户端
- 若 `model.api_key` 为空，使用占位客户端

### 真实客户端行为

真实客户端会向以下地址发送请求：

```text
{base_url}/chat/completions
```

请求格式为：

- `Authorization: Bearer <api_key>`
- `Content-Type: application/json`
- `Accept: text/event-stream`

并按 SSE 的 `data:` 分片持续读取返回内容。

### 配置示例

```yaml
model:
  provider: openai
  name: gpt-4.1
  streaming: true
  base_url: https://api.openai.com/v1
  api_key: sk-xxxx
```

### 兼容范围

当前实现兼容这类 OpenAI 风格返回：

- `choices[0].delta.content` 为字符串
- `choices[0].delta.content` 为内容分片数组

### 注意事项

- 当前只实现了 Chat Completions 流式路径
- 若服务端返回的流片段不是标准 JSON / SSE 格式，会触发流错误
- 若 API Key 错误、网络错误、HTTP 4xx/5xx，会进入章节级失败与重试流程

---

## 11. 运行测试

执行单元测试：

```bash
python -m unittest discover -s tests -v
```

当前测试覆盖：

- 工作区路径生成
- schema 加载
- schema 校验
- prompt 渲染
- YAML 增量合并
- API 客户端辅助函数
- API 客户端构造逻辑

---

## 12. 常见问题

### Q1：为什么运行后没有结果？
通常是因为 `config.yaml` 中的 `input_epubs` 还是空列表。

### Q2：为什么输出都是空列表？
通常是因为 `model.api_key` 为空，系统回退到了占位客户端。

### Q3：为什么 `schemas/characters.yaml` 这种文件也能加载？
因为 `src/schema_loader.py` 中增加了对说明性占位 schema 的回退解析逻辑，不完全依赖标准 `PyYAML` 成功解析。

### Q4：为什么没有写入数据库？
这是设计要求之一。当前版本状态全部落在文件系统中。

### Q5：可以只跑某一个 schema 吗？
可以。只要在 `config.yaml` 的 `schema_paths` 中保留目标 schema 即可。

### Q6：可以接入代理服务或兼容 OpenAI 协议的网关吗？
可以。只要它支持 `/chat/completions` 的流式返回，并且 `base_url` 可配置即可。

---

## 13. 推荐使用流程

建议按下面的顺序使用：

1. 准备一个或多个 EPUB 到 `input/`
2. 修改 `config.yaml` 中的 `input_epubs`
3. 先在不填 `api_key` 的情况下运行 `python src/app.py` 验证本地流程
4. 确认 `workspace/` 结构、状态文件和输出文件都已生成
5. 再填入真实 `api_key` 和 `base_url`
6. 用单本 EPUB + 单个 schema 进行真实 API 联调
7. 最后再开启多 EPUB / 多 schema 并发

---

## 14. 后续扩展建议

后续如果继续推进，可以优先做这些事情：

1. 增加环境变量优先级与安全的密钥读取策略
2. 支持更多 OpenAI-compatible 响应差异格式
3. 完善 `logs/` 日志落盘
4. 增强 schema 注释提取与错误摘要可读性
5. 增加更多针对 `task_runner` 的集成测试
6. 再考虑是否引入 LangChain / LangGraph 封装
