# -*- coding: utf-8 -*-
"""
主挖掘程序（模板化版）
依赖：
- machine_lib.py 中已加入 MachinelibTemplates 类
- config.py, fields.py 与原工程保持一致
"""

import os
import random
import time
import asyncio
from datetime import datetime
import argparse

from machine_lib_v2 import *                    # 你原有的API：login/get_datafields/process_datafields/...
from machine_lib_v2 import MachinelibTemplates  # 模板生成器类
from config import *                         # 你的常规配置
from fields import *                         # 字段映射等

from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn
from rich.console import Console

console = Console()


# ---------------------- 基于模板的候选生成 ----------------------
def _pick_template_alphas(model_type: str, group) -> list:
    """
    从字段集合 `group` 生成表达式；放大产量的版本。
    支持:
      - momentum_diverse      扩大窗口/分组
      - twin / risk_compare   同上
      - combo_core            合并三类模板，产量中高
      - combo_heavy           字段/窗口/分组全放大（警惕过多）
    """
    # 1) 拉平 + 去重
    all_fields = list(dict.fromkeys(
        process_datafields(group, "matrix") + process_datafields(group, "vector")
    ))

    def filt(keys):
        kl = [k.lower() for k in keys]
        return [f for f in all_fields if any(k in f.lower() for k in kl)]

    # 字段族（放宽筛选，不只 returns/close）
    core_fields  = filt(["returns", "ret", "close", "open", "high", "low", "volume", "turnover", "cap"])
    if len(core_fields) < 50:
        # 字段不够就把前 200 个都拉上，增加覆盖面
        core_fields = (core_fields + all_fields)[:800]
    twin_fields  = filt(["volume", "turnover", "cap", "liquidity", "dollar"])
    risk_fields  = filt(["beta", "vol", "risk", "downside", "skew", "kurt"]) or core_fields[:50]
    news_fields  = filt(["nws", "news", "snt", "sentiment"])
    analyst_fields = filt(["anl", "analyst", "estimate", "recom"]) or core_fields[:50]
    pv_fields    = [x for x in all_fields if "close*volume" in x.lower()] or ["close*volume"]

    # 2) 分组与窗口（放大）
    GROUPS_LITE   = ["sector", "industry"]
    GROUPS_FULL   = MachinelibTemplates.get_builtin_groups()  # 市场/国家/行业层级 + 各类分桶
    SHORT_BIG     = [5, 10, 20, 22, 33]                       # 之前只有 5,22
    LONG_BIG      = [66, 90, 120, 180, 252, 504]              # 之前只有 66,120
    LONG_MEDIUM   = [66, 120, 252]

    key = (model_type or "").strip().lower().replace(" ", "_")
    out = []

    if key in ("momentum_diverse", "momentum", "mom_div", ""):
        out = MachinelibTemplates.build_momentum_diverse(
            fields=core_fields,
            groups=GROUPS_FULL,                 # 扩到全组
            short_windows=SHORT_BIG,            # 扩到 5 个短窗
            long_windows=LONG_BIG,              # 扩到 6 个长窗
            wrap=(120, 4.0, True)
        )

    elif key in ("twin", "pair", "corr"):
        out = MachinelibTemplates.build_twin_ops(
            primary_fields=core_fields[:80],    # 控制对数，避免组合爆炸
            twin_fields=(twin_fields or core_fields)[:80],
            windows=LONG_MEDIUM,
            twin_ops=["ts_corr", "ts_covariance"],
            groups=GROUPS_FULL,
            wrap=(120, 4.0, True)
        )

    elif key in ("risk_compare", "risk_group"):
        out = MachinelibTemplates.build_risk_group_compare(
            risk_fields=risk_fields[:120],
            groups=GROUPS_FULL,
            compare_ops=MachinelibTemplates.GROUP_COMPARE_OPS,
            wrap=(120, 4.0, True)
        )

    elif key in ("combo_core",):
        # 合并三类模板（数量通常几千到一两万，视字段而定）
        a1 = MachinelibTemplates.build_momentum_diverse(
            fields=core_fields,
            groups=GROUPS_FULL,
            short_windows=SHORT_BIG,
            long_windows=LONG_MEDIUM,
            wrap=(120, 4.0, True)
        )
        a2 = MachinelibTemplates.build_twin_ops(
            primary_fields=core_fields[:60],
            twin_fields=(twin_fields or core_fields)[:60],
            windows=LONG_MEDIUM,
            twin_ops=["ts_corr", "ts_covariance"],
            groups=GROUPS_FULL,
            wrap=(120, 4.0, True)
        )
        a3 = MachinelibTemplates.build_risk_group_compare(
            risk_fields=risk_fields[:100],
            groups=GROUPS_FULL,
            compare_ops=["group_neutralize", "group_rank", "group_zscore"],
            wrap=(120, 4.0, True)
        )
        out = list(dict.fromkeys(a1 + a2 + a3))  # 去重保持顺序

    elif key in ("combo_heavy",):
        # 重口味：字段/分组/窗口全拉满（小心 OOM）
        a1 = MachinelibTemplates.build_momentum_diverse(
            fields=core_fields[:300],
            groups=GROUPS_FULL,
            short_windows=SHORT_BIG,
            long_windows=LONG_BIG,
            wrap=(120, 4.0, True)
        )
        a2 = MachinelibTemplates.build_twin_ops(
            primary_fields=core_fields[:150],
            twin_fields=(twin_fields or core_fields)[:150],
            windows=LONG_BIG,
            twin_ops=["ts_corr", "ts_covariance"],
            groups=GROUPS_FULL,
            wrap=(120, 4.0, True)
        )
        a3 = MachinelibTemplates.build_risk_group_compare(
            risk_fields=risk_fields[:200],
            groups=GROUPS_FULL,
            compare_ops=MachinelibTemplates.GROUP_COMPARE_OPS,
            wrap=(120, 4.0, True)
        )
        out = list(dict.fromkeys(a1 + a2 + a3))

    elif key in ("news_corr", "news_volume"):
        if not news_fields:
            print(datetime.now(), "未发现新闻字段，退回 momentum_diverse")
            return _pick_template_alphas("momentum_diverse", group)
        out = MachinelibTemplates.build_news_return_corr(
            news_fields=news_fields[:60],
            windows=[120, 180, 252],
            groups=GROUPS_FULL,
            wrap=(120, 4.0, True)
        )

    elif key in ("analyst_reg", "analyst_regression"):
        out = MachinelibTemplates.build_analyst_regression(
            analyst_fields=analyst_fields[:80],
            pv_fields=list(dict.fromkeys(pv_fields + ["close*volume"]))[:10],
            w1=22, w2=120,
            groups=["country", "industry", "sector"],
            wrap=(120, 4.0, True)
        )

    elif key in ("fcf", "fundamental_fcf"):
        out = MachinelibTemplates.build_fcf_ratio(
            fcf_field="fcf", mkt_cap_field="market_cap",
            smooth_window=60, groups=["sector","industry"], wrap=(120, 4.0, True)
        )

    elif key in ("vector_neut", "risk_neutral"):
        risk_ref = next((x for x in all_fields if "risk" in x.lower()), "risk70")
        out = MachinelibTemplates.build_vector_neutralized(
            fields=core_fields[:150],
            risk_field=risk_ref,
            windows=[5,10,20,22,33],
            groups_after=["sector", "bucket(rank(cap), range='0.1, 1, 0.1')"],
            wrap=(120, 4.0, True)
        )

    elif key in ("explore", "smoke"):
        out = MachinelibTemplates.build_explore_simple(core_fields[:200], wrap=(120, 4.0, True))

    else:
        # 未识别：默认用放大的动量分歧
        out = MachinelibTemplates.build_momentum_diverse(
            fields=core_fields,
            groups=GROUPS_FULL,
            short_windows=SHORT_BIG,
            long_windows=LONG_BIG,
            wrap=(120, 4.0, True)
        )

    # 最后一道去重
    return list(dict.fromkeys(out))


# ---------------------- 主任务 ----------------------

def run_task(dataset_id, region, delay, instrumentType, universe, n_jobs, tag=None, model_type="momentum_diverse"):
    delay = int(delay)
    n_jobs = int(n_jobs)

    print(datetime.now(), "================= 回测任务启动 =================")
    print(datetime.now(), f"dataset_id:       {dataset_id}")
    print(datetime.now(), f"region:           {region}")
    print(datetime.now(), f"delay:            {delay}")
    print(datetime.now(), f"instrumentType:   {instrumentType}")
    print(datetime.now(), f"universe:         {universe}")
    print(datetime.now(), f"n_jobs:           {n_jobs}")
    print(datetime.now(), f"tag:              {tag}")
    print(datetime.now(), f"model_type:       {model_type}")
    print("================================================")

    print(datetime.now(), "登录中...")
    s = login()
    print(datetime.now(), "登录成功，拉取字段中...")

    group = get_datafields(s=s, dataset_id=dataset_id, region=region, delay=delay, universe=universe)
    s.close()

    if group is None or len(group) == 0:
        print(datetime.now(), "字段为空，跳过任务")
        return

    if tag is None:
        tag = f"{region}_{delay}_{instrumentType}_{universe}_{dataset_id}_{model_type}"

    completed_file_path = os.path.join(RECORDS_PATH, f"{tag}_simulated_alpha_expression.txt")
    completed_alphas = read_completed_alphas(completed_file_path)

    # 基于模板生成候选
    print(datetime.now(), f"🧱 使用模板 [{model_type}] 生成候选表达式...")
    raw_alpha_list = _pick_template_alphas(model_type=model_type, group=group)
    print(datetime.now(), f"✅ 模板生成完成，共 {len(raw_alpha_list)} 条")

    # 过滤已完成
    alpha_list = [alpha for alpha in raw_alpha_list if alpha not in completed_alphas]

    if len(alpha_list) == 0:
        print(datetime.now(), f"{tag} 所有表达式已完成，跳过")
        return

    # 打乱+截断
    random.shuffle(alpha_list)
    alpha_list = alpha_list[:1000]  # 防卡死保险

    print(datetime.now(), f"🎯 待回测表达式数：{len(alpha_list)} / {len(raw_alpha_list)}")

    # 组装提交参数
    region_list = [(region, universe)] * len(alpha_list)
    decay_list = [random.randint(0, 10) for _ in alpha_list]
    delay_list = [delay] * len(alpha_list)
    neut = 'SUBINDUSTRY'

    batch_size = 80
    print(datetime.now(), f"⏩ 共需回测：{len(alpha_list)} 个，批大小：{batch_size}")

    with Progress(
        TextColumn("[bold green]回测进度"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        transient=True
    ) as progress:
        task = progress.add_task("提交中...", total=len(alpha_list))

        for i in range(0, len(alpha_list), batch_size):
            batch = alpha_list[i:i + batch_size]
            region_batch = region_list[i:i + batch_size]
            decay_batch = decay_list[i:i + batch_size]
            delay_batch = delay_list[i:i + batch_size]

            console.print(f"{datetime.now()} 提交第 {i // batch_size + 1} 批（{len(batch)} 条）...")

            asyncio.run(simulate_multiple_tasks(
                batch, region_batch, decay_batch, delay_batch,
                tag, neut, [], n=n_jobs
            ))

            progress.update(task, advance=len(batch))
            time.sleep(15)

    print(datetime.now(), "✅ 本轮任务完成，等待10秒...")
    time.sleep(10)
    print(datetime.now(), "开始下一轮")


# ---------------------- 启动入口 ----------------------
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="模板化挖掘器")
    parser.add_argument("--dataset_id", default="news18")
    parser.add_argument("--region", default="EUR")
    parser.add_argument("--delay", type=int, default=1)
    parser.add_argument("--instrumentType", default="EQUITY")
    parser.add_argument("--universe", default="TOP2500")
    parser.add_argument("--n_jobs", type=int, default=6)
    parser.add_argument("--tag", default="news18model")
    parser.add_argument("--model_type", default="news_corr",
                        help="option1 | momentum_diverse | twin | vol_div | risk_compare | vector_neut | mean_dev | news_corr | fcf | analyst_reg | explore")
    parser.add_argument("--loop", action="store_true", help="是否循环执行")

#模板类型解释：

#多个数据字段的

# option1（期权IV差 + 门控）
# 模板：trade_when(pcr_oi_d>1, group_neutralize(ts_delta(IV_call - IV_put, L), densify(G)), -1)
# 含义：看多/看空偏度的短期变化，在行业或细分组内做中性化，PCR>1时才生效，降噪。
# 字段建议：iv_call_*、iv_put_*（日变动/水平），可选 pcr_oi/pcr_vol；缺少即别用此模式。

# momentum_diverse（动量分歧）
# 模板：[group_zscore(ts_zscore(X, w1), G) - group_zscore(ts_zscore(X, w2), G)] 且 w2>w1
# 含义：短期与中长期信号的差分，捕捉反转或加速段。
# 字段建议：returns、close，也可用 volume/close、turnover 派生序列。

# twin（成对相关/协方差）
# 模板：group_neutralize(ts_corr(X, Y, L) | ts_covariance(X, Y, L), densify(G))
# 含义：两变量在时间窗内的联动强度，组内去除结构性差异。
# 字段建议：(returns, volume)、(returns, cap)、(close, volume) 等常见量价/规模对。

# vol_div（波动率分歧）
# 模板：group_neutralize(power(ts_mean(abs(X), L),2) - power(ts_mean(X, L),2), densify(G))
# 含义：幅度 vs 方向的能量差，偏向捕捉震荡与噪声主导段。
# 字段建议：returns 为主；可试 spread(ask,bid)、hi_lo_range 等波动代理。

# vector_neut（向量中性化）
# 模板：wrap(vector_neut(ts_zscore(X, L), ts_backfill(RISK_VEC, 120)))，可再对 G 多级中性化
# 含义：从目标信号中剥离给定风险向量的暴露，保留“纯阿尔法”。
# 字段建议：X=returns/close；RISK_VEC 用综合风险如 risk70、或拼接的风格贝塔集合。

# fcf（自由现金流率）
# 模板：group_neutralize(-ts_mean(winsorize(ts_backfill(FCF/MC,120), std=4), 60), densify(G))
# 含义：价值/现金回报偏好，反向取值以适配做多“便宜”资产。
# 字段建议：fcf、market_cap 或等价口径的 FCF 与市值字段；注意口径一致性与滞后。

# analyst_reg（分析师回归）
# 模板：-ts_mean(group_neutralize(ts_regression(ts_zscore(ANL,w1), ts_zscore(PV,w1), w2), densify(G)), w2)
# 含义：分析师变量对价格/成交强度的解释残差，组内标准化后平滑。
# 字段建议：ANL=anl* 家族（上调/下调、覆盖、一致预期变化等）；PV=close*volume 或 dollar_volume。




#单个数据字段就可以的

# risk_compare（风险分组比较）
# 模板：group_{rank|zscore|neutralize|scale}(RISK, densify(G))
# 含义：直接在分组维度上比较风险暴露并标准化。
# 字段建议：beta_60/120、vol_20/60、idiorisk、downside_vol、skew/kurt 等风险因子。

# mean_dev（均值偏离）
# 模板：group_neutralize((X - ts_mean(X, L))/max(abs(ts_mean(X, L)), eps), densify(G))
# 含义：相对自身均值的偏离度，适合均值回归/过度延伸识别。
# 字段建议：returns、close/ma、factor_raw 等单变量时序。

# news_corr（新闻×收益相关）
# 模板：group_neutralize(ts_corr(NEWS, returns, L), densify(G))
# 含义：情绪与价格的耦合强度，偏向中期。
# 字段建议：nws*、snt*、news18/77 这类新闻/情绪聚合字段；没有就别选这模式。

# explore（操你妈模板）
# 模板：wrap(zscore(ts_delta(rank(ts_zscore(X,60)), 5)))
# 含义：快速操你妈的二层动量框架，用于筛底噪字段的可用性。
# 字段建议：returns 起步；也可用任意“可疑但想试”的原始序列。


    args = parser.parse_args()

    if args.loop:
        while True:
            run_task(
                dataset_id=args.dataset_id,
                region=args.region,
                delay=args.delay,
                instrumentType=args.instrumentType,
                universe=args.universe,
                n_jobs=args.n_jobs,
                tag=args.tag,
                model_type=args.model_type
            )
    else:
        run_task(
            dataset_id=args.dataset_id,
            region=args.region,
            delay=args.delay,
            instrumentType=args.instrumentType,
            universe=args.universe,
            n_jobs=args.n_jobs,
            tag=args.tag,
            model_type=args.model_type
        )
