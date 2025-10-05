# -*- coding: utf-8 -*-
"""
dig_xy_template_fixed.py
用途：为模板
  alpha = ( rank(ts_zscore(x,252) - ts_zscore(y,252)) - 0.5 + ts_delta(rank(...),5) )
          * ts_arg_max(ts_delta(abs(x),1) > 0, 61)
批量生成跨数据集表达式并提交回测。参数写死在文件顶部，便于像 DIG1 一样手改。

依赖：machine_lib.py（同目录）
记录：records/{TAG}_simulated_alpha_expression.txt（自动创建）
限制：严格遵守 8 并发、每批最多 80 条（每 task 内部最多 10 条）的平台规则
"""

import os
import time
import random
import asyncio
from itertools import product
from datetime import datetime

# ==== 配置区：像 DIG1 那样在这里改 ====
REGION = "USA"
UNIVERSE = "TOP3000"
INSTRUMENT_TYPE = "EQUITY"
DELAY = 1

# x 选“快变量”数据集，y 选“慢锚”数据集。按需增删。
X_DATASETS = ["option8", "option14"]      # 示例：["news83", "option8"]
Y_DATASETS = ["analyst69", "fundamental6"]  # 示例：["analyst69", "fundamental6"]

MAX_X_FIELDS = 200     # 限制每侧字段量，防止组合爆炸
MAX_Y_FIELDS = 200
MAX_PAIRS = 1000       # 最多生成多少条表达式并提交
N_JOBS = 6             # 并发（内部仍会限制 <=8）
NEUT = "SUBINDUSTRY"   # 中性化层级
TAG = "xy_template_v1" # 记录文件标识
SLEEP_BETWEEN_BATCHES = 3  # 批次之间休眠秒数
COOLDOWN_AFTER_ROUND = 10   # 整轮结束冷却秒数
BATCH_UPPER_BOUND =  8     # 平台单轮提交上限（保持 80）
# =====================================

# ===== 依赖你上传的工具库 =====
from machine_lib import (
    login,
    get_datafields,
    process_datafields,
    simulate_multiple_tasks,
    read_completed_alphas,
)

# ===== 基础工具 =====
def log(msg: str):
    print(f"{datetime.now()} {msg}", flush=True)

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

# ===== 表达式构造 =====
def build_alpha_expr(x_field: str, y_field: str) -> str:
    """
    内联展开，避免变量赋值在 FASTEXPR 中的兼容性问题。
    注：x_field/y_field 需已被 process_datafields 处理为可直接放入表达式的片段。
    """
    r = f"(rank(ts_zscore({x_field}, 252) - ts_zscore({y_field}, 252)) - 0.5)"
    r_delta = f"ts_delta({r}, 5)"
    gate = f"ts_arg_max(ts_delta(abs({x_field}), 1) > 0, 61)"
    expr = f"({r} + {r_delta}) * {gate}"
    return expr

# ===== 字段获取与检查 =====
def fetch_fields_for_dataset(s, dataset_id: str, region: str, delay: int, universe: str,
                             instrument_type: str = "EQUITY"):
    """
    拉取数据集字段，套用你库里的清洗封装：
    process_datafields(df, "matrix") + process_datafields(df, "vector")
    并输出调试日志，直观看到 rows/类型/sample ids。
    """
    df = get_datafields(
        s=s,
        instrument_type=instrument_type,
        region=region,
        delay=delay,
        universe=universe,
        dataset_id=dataset_id,
        search=""
    )

    try:
        cnt = 0 if df is None else len(df)
        types = {} if df is None else df['type'].value_counts().to_dict()
        heads = [] if df is None else df['id'].head(5).tolist()
        log(f"[DEBUG] dataset={dataset_id} region={region} universe={universe} delay={delay} "
            f"type={instrument_type} -> rows={cnt}, types={types}, sample={heads}")
    except Exception as e:
        log(f"[DEBUG] inspect df failed for dataset={dataset_id}: {e}")

    if df is None or len(df) == 0:
        return []

    fields = process_datafields(df, "matrix") + process_datafields(df, "vector")
    # 去重
    fields = list(dict.fromkeys(fields))
    return fields

def _norm_key(expr_fragment: str) -> str:
    """
    用于剥离 winsorize/ts_backfill 包装，降低 x/y 完全同源时的同名碰撞。
    """
    k = expr_fragment
    for token in ["winsorize(", "ts_backfill(", ")", ", 120"]:
        k = k.replace(token, "")
    return k

def pick_fast_slow_fields(s,
                          region: str,
                          delay: int,
                          universe: str,
                          x_datasets: list,
                          y_datasets: list,
                          max_x: int,
                          max_y: int,
                          instrument_type: str = "EQUITY"):
    x_fields, y_fields = [], []

    for ds in x_datasets:
        x_fields.extend(fetch_fields_for_dataset(s, ds, region, delay, universe, instrument_type))
    for ds in y_datasets:
        y_fields.extend(fetch_fields_for_dataset(s, ds, region, delay, universe, instrument_type))

    random.shuffle(x_fields)
    random.shuffle(y_fields)
    x_fields = x_fields[:max_x]
    y_fields = y_fields[:max_y]

    # 降低同源重复：避免 x 与 y 完全同 key
    y_keys = {_norm_key(y) for y in y_fields}
    x_fields = [x for x in x_fields if _norm_key(x) not in y_keys]

    return x_fields, y_fields

# ===== 主流程 =====
def run_once():
    ensure_dir("records")
    completed_file = os.path.join("records", f"{TAG}_simulated_alpha_expression.txt")
    completed_alphas = read_completed_alphas(completed_file)

    log("登录 WQB...")
    s = login()
    log("登录成功，开始拉取字段列表")

    # 拉字段
    x_fields, y_fields = pick_fast_slow_fields(
        s=s,
        region=REGION,
        delay=DELAY,
        universe=UNIVERSE,
        x_datasets=X_DATASETS,
        y_datasets=Y_DATASETS,
        max_x=MAX_X_FIELDS,
        max_y=MAX_Y_FIELDS,
        instrument_type=INSTRUMENT_TYPE
    )
    try:
        s.close()
    except Exception:
        pass

    if not x_fields or not y_fields:
        log(f"字段为空：x={len(x_fields)} y={len(y_fields)}。检查权限/region/universe/delay/dataset id")
        return

    # 组合表达式
    exprs = []
    for xf, yf in product(x_fields, y_fields):
        expr = build_alpha_expr(xf, yf)
        if expr not in completed_alphas:
            exprs.append(expr)

    random.shuffle(exprs)
    if MAX_PAIRS > 0:
        exprs = exprs[:MAX_PAIRS]

    if not exprs:
        log(f"无新增表达式。x_fields={len(x_fields)}, y_fields={len(y_fields)}")
        return

    log(f"待回测表达式：{len(exprs)}  (x_fields={len(x_fields)}, y_fields={len(y_fields)}), TAG={TAG}")

    # 参数打包
    regions = [(REGION, UNIVERSE)] * len(exprs)
    decays = [random.randint(0, 10) for _ in exprs]
    delays = [DELAY] * len(exprs)

    # 分批提交（每批最多 80）
    total = len(exprs)
    submitted = 0
    batch_id = 0
    while submitted < total:
        batch_id += 1
        batch = exprs[submitted:submitted + BATCH_UPPER_BOUND]
        rb = regions[submitted:submitted + BATCH_UPPER_BOUND]
        db = decays[submitted:submitted + BATCH_UPPER_BOUND]
        lb = delays[submitted:submitted + BATCH_UPPER_BOUND]

        log(f"提交第 {batch_id} 批，数量 {len(batch)} 条（已提交 {submitted}/{total}）")
        # 注意：simulate_multiple_tasks 内部应限制每 task<=10 条，并发<=N_JOBS
        asyncio.run(simulate_multiple_tasks(
            batch, rb, db, lb, TAG, NEUT, [], n=N_JOBS
        ))

        submitted += len(batch)
        if submitted < total:
            log(f"批次冷却 {SLEEP_BETWEEN_BATCHES}s...")
            time.sleep(SLEEP_BETWEEN_BATCHES)

    log(f"本轮完成，总计 {total} 条。整轮冷却 {COOLDOWN_AFTER_ROUND}s")
    time.sleep(COOLDOWN_AFTER_ROUND)

def main():
    while True:
        try:
            run_once()
        except Exception as e:
            log(f"发生错误：{e}。2 秒后重试")
            time.sleep(2)

if __name__ == "__main__":
    main()
