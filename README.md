# 大坝巡检、缺陷与应急管理

安排巡检，记录渗流、位移、裂缝等缺陷并跟踪修复、复检和应急预案。

两队汛期夜巡断网时各自改任务，回网后按任务号合并缺陷。巡检任务、缺陷和审计链共用同一份可续传快照：同一条缺陷两边都改过时保留候选，复核前不能关闭或派发应急任务；同时提交先到结果生效，后到内容按请求号留冲突；重复导入沿用第一次结果。写入失败后按检查点恢复重试，已有数据缺检查点时回填快照，审计轨迹仍可查。

## 模块结构

- `app.py`：参数解析、依赖组装和HTTP服务启动。
- `src/domain.py`：数据结构、错误、状态和基础校验。
- `src/rules.py`：状态机、角色矩阵、优先级、期限和关闭不变量。
- `src/repository.py`：SQLite建表、事务、版本控制、审计链、快照/请求号/冲突/进度表与回填。
- `src/service.py`：权限检查、用例编排、并发控制、审计、离线合并与断点续传。
- `src/snapshot.py`：可续传快照的检查点、打包与哈希链。
- `src/merge.py`：按任务号合并的纯领域逻辑与冲突候选。
- `src/http_api.py`：JSON路由和统一错误响应。
- `src/audit.py`：UTC时间和SHA-256审计事件。
- `static/index.html`：最小演示页。
- `tests/`：完整流程、规则、失败与快照合并测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8316
```

默认端口为`8316`，首次启动自动建库并为缺检查点的存量数据回填快照。使用`X-Actor`和`X-Role`请求头传递身份。

## 主要接口

- `GET /health`
- `GET /api/items`
- `POST /api/items`
- `GET /api/items/{id}`
- `POST /api/items/{id}/records`
- `POST /api/items/{id}/transition`，必须提交`expected_version`
- `POST /api/items/{id}/emergency-dispatch`，冲突复核前不可用
- `GET /api/audit`
- `GET /api/snapshots`：列出可续传快照
- `POST /api/snapshots/import`：离线变更导入，请求体含`request_no`、`base_checkpoint`、`changes`
- `POST /api/snapshots/backfill`：手动回填存量快照
- `GET /api/conflicts`：列出冲突候选（可加`?status=pending_review`）
- `POST /api/conflicts/{id}/review`：复核冲突，请求体含`winning_request_no`

允许角色：inspector, dam_engineer, emergency_manager, viewer。异常值比控制阈值越高，缺陷优先级越高；应急处置缺陷必须完成复检并记录证据后才能关闭。

## 离线合并规则

- **按任务号合并**：`changes` 中每条缺陷以 `external_ref`（任务号）为键。
- **请求号幂等**：同一 `request_no` 重复导入沿用第一次结果，不重复写入。
- **先到生效**：基于最新检查点的顺序提交正常覆盖；两边基于同一分支点都改了同一条缺陷时，先到的版本先生效。
- **候选冲突**：后到的内容按请求号保留为候选（`pending_review`），不覆盖线上版本；复核前该缺陷不能关闭、不能派发应急任务。
- **复核解冻**：`POST /api/conflicts/{id}/review` 指定 `winning_request_no` 后，选中的候选生效，缺陷恢复正常。
- **断点续传**：写入按 `received → classified → applied → finalized` 检查点落盘，并按缺陷记录进度；失败后用同一 `request_no` 重试即从断点继续，已写部分不重复。
- **回填**：启动时自动为缺检查点的存量数据补快照，审计链不被改写、仍可查询验证。

## 测试

```bash
python3 -m unittest discover -s tests -v
```
