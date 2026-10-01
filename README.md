# 大坝巡检、缺陷与应急管理

安排巡检，记录渗流、位移、裂缝等缺陷并跟踪修复、复检和应急预案。支持两队汛期夜巡断网作业、回网按任务号合并。

## 模块结构

- `app.py`：参数解析、依赖组装和HTTP服务启动。
- `src/domain.py`：数据结构、错误、状态和基础校验。
- `src/rules.py`：状态机、角色矩阵、优先级、期限、关闭/复核不变量。
- `src/repository.py`：SQLite建表、事务、版本控制、同步检查点和审计链。
- `src/service.py`：权限检查、用例编排、并发控制、复核闸门和审计。
- `src/sync.py`：断网续传批处理（请求号幂等、检查点重放）。
- `src/http_api.py`：JSON路由和统一错误响应。
- `src/audit.py`：UTC时间和SHA-256审计事件。
- `static/index.html`：最小演示页。
- `tests/`：完整流程、规则、失败与断网续传测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8316
```

默认端口为`8316`，首次启动自动建库；打开已有库时若发现业务数据缺少快照检查点，会自动幂等回填。使用`X-Actor`和`X-Role`请求头传递身份。

## 主要接口

- `GET /health`
- `GET /api/items` / `POST /api/items` / `GET /api/items/{id}`
- `POST /api/items/{id}/records`
- `POST /api/items/{id}/transition`，必须提交`expected_version`
- `GET /api/audit`

### 断网续传与合并

- `POST /api/sync`：回网批量提交，`request_no`为请求号，`ops[]`支持
  - `task_upsert`：巡检任务按`task_no`合并（改期/改线携带`base_version`）
  - `defect_upsert`：缺陷按`external_ref`合并（携带`base_version`）
  - `defect_record`：缺陷现场记录按`record_ref`幂等导入
- `GET /api/tasks`：巡检任务列表
- `GET /api/candidates?status=pending`：合并冲突候选（dam_engineer/emergency_manager）
- `POST /api/candidates/{id}/resolve`：复核，`decision`为`accept`或`reject`
- `POST /api/items/{id}/dispatch`：派发应急任务（emergency_manager）
- `GET /api/dispatches`
- `GET /api/snapshot?after=<seq>`：共用续传快照水位
- `POST /api/snapshot/backfill`：为已有数据回填快照

## 合并与续传规则

- **共用快照**：巡检任务、缺陷和审计事件的每次有效写入在同一事务内追加一条`snapshot_log`，`seq`即续传水位。
- **先到生效**：同一条缺陷两边都基于同一版本改过，先到的提交更新生效；后到内容不覆盖，按`request_no`保留为冲突候选。
- **复核闸门**：存在待复核候选的缺陷不能关闭，也不能派发应急任务；复核（采纳/驳回）后解除。
- **请求号幂等**：同一`request_no`重复导入直接沿用第一次的批次结果；每条操作的检查点为`(request_no, op_index)`。
- **检查点恢复**：写入失败返回`503`并携带`request_no`与`checkpoint`，客户端用同一请求号重放，已完成的操作跳过、未完成的补做。
- **回填与审计**：已有数据缺检查点时回填快照（幂等），回填本身也入审计；审计哈希链始终可经`GET /api/audit`查验。

允许角色：inspector, dam_engineer, emergency_manager, viewer。异常值比控制阈值越高，缺陷优先级越高；应急处置缺陷必须完成复检并记录证据后才能关闭。

## 测试

```bash
python3 -m unittest discover -s tests -v
```
