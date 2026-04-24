from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from .common import ensure_dir, parse_as_of_month
from .settings import OUTPUT_DIR, PIPELINE_CONFIG, PRESENTATION_DIR


DEFAULT_DPI = 220


def _resolve_layer(base_dir: Path, layer_name: str, required_file: str) -> Path:
    candidates = [base_dir / layer_name, base_dir]
    for candidate in candidates:
        if (candidate / required_file).exists():
            return candidate
    raise FileNotFoundError(f"Cannot find {required_file} in {base_dir}/{layer_name} or {base_dir}")


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _save_fig(fig: plt.Figure, out_path: Path, dpi: int = DEFAULT_DPI) -> None:
    fig.tight_layout()
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)


def _clean_gate_name(raw: str) -> str:
    x = str(raw or "").strip()
    x = x.replace("gate_", "")
    x = x.replace("_", " ")
    return x.title()


def _pipeline_overview_chart(base_dir: Path, out_dir: Path) -> dict[str, float]:
    stages = []

    raw_metrics = base_dir / "raw" / "load_metrics_raw.json"
    if raw_metrics.exists():
        data = json.loads(raw_metrics.read_text(encoding="utf-8"))
        raw_rows = float(sum(item.get("rows_written", 0) for item in data.get("sources", [])))
        stages.append(("Raw", raw_rows))

    norm_metrics = base_dir / "normalized" / "load_metrics_normalized.json"
    if norm_metrics.exists():
        data = json.loads(norm_metrics.read_text(encoding="utf-8"))
        norm_rows = float(sum(item.get("rows", 0) for item in data.get("tables", [])))
        stages.append(("Normalized", norm_rows))

    mart_metrics = base_dir / "mart" / "load_metrics_mart.json"
    if mart_metrics.exists():
        data = json.loads(mart_metrics.read_text(encoding="utf-8"))
        mart_rows = float(sum(item.get("rows", 0) for item in data.get("tables", [])))
        stages.append(("Mart", mart_rows))

    seg_metrics = base_dir / "segmentation" / "load_metrics_segmentation.json"
    if seg_metrics.exists():
        data = json.loads(seg_metrics.read_text(encoding="utf-8"))
        seg_rows = float(sum(item.get("rows", 0) for item in data.get("tables", [])))
        stages.append(("Segmentation", seg_rows))

    uplift_metrics = base_dir / "uplift" / "load_metrics_uplift.json"
    if uplift_metrics.exists():
        data = json.loads(uplift_metrics.read_text(encoding="utf-8"))
        uplift_rows = float(sum(item.get("rows", 0) for item in data.get("tables", [])))
        stages.append(("Uplift", uplift_rows))

    if not stages:
        return {}

    df = pd.DataFrame(stages, columns=["layer", "rows"])
    fig, ax = plt.subplots(figsize=(10, 5.4))
    sns.barplot(data=df, x="layer", y="rows", hue="layer", palette="crest", dodge=False, legend=False, ax=ax)
    ax.set_title("Объем данных по слоям пайплайна", fontsize=16, pad=14)
    ax.set_xlabel("")
    ax.set_ylabel("Количество строк")
    ax.ticklabel_format(axis="y", style="plain")

    for i, v in enumerate(df["rows"]):
        ax.text(i, v, f"{int(v):,}".replace(",", " "), ha="center", va="bottom", fontsize=10)

    _save_fig(fig, out_dir / "01_pipeline_layers_rows.png")
    return {str(row["layer"]): float(row["rows"]) for _, row in df.iterrows()}


def _dq_chart(normalized_dir: Path, out_dir: Path) -> dict[str, int]:
    dq = _safe_read_csv(normalized_dir / "dq_issues.csv")
    if dq.empty or "issue_type" not in dq.columns:
        return {}

    agg = dq.groupby("issue_type", as_index=False).size().sort_values("size", ascending=False)
    fig, ax = plt.subplots(figsize=(10.5, 5.8))
    sns.barplot(data=agg, y="issue_type", x="size", hue="issue_type", palette="mako", dodge=False, legend=False, ax=ax)
    ax.set_title("Контроль качества данных: распределение DQ-ошибок", fontsize=16, pad=14)
    ax.set_xlabel("Количество записей")
    ax.set_ylabel("Тип DQ")

    for idx, row in agg.reset_index(drop=True).iterrows():
        ax.text(row["size"], idx, f" {int(row['size'])}", va="center", ha="left", fontsize=10)

    _save_fig(fig, out_dir / "02_dq_issue_distribution.png")
    return {str(r["issue_type"]): int(r["size"]) for _, r in agg.iterrows()}


def _hybrid_gate_share_chart(seg_dir: Path, out_dir: Path) -> dict[str, float]:
    gates = _safe_read_csv(seg_dir / "segment_hybrid_gate_profile.csv")
    if gates.empty:
        return {}

    gates = gates.copy()
    gates["gate_pretty"] = gates["business_gate"].map(_clean_gate_name)
    gates = gates.sort_values("share_total_pct", ascending=False)

    fig, ax = plt.subplots(figsize=(11.5, 6.2))
    sns.barplot(data=gates, y="gate_pretty", x="share_total_pct", hue="gate_pretty", palette="viridis", dodge=False, legend=False, ax=ax)
    ax.set_title("Гибридные бизнес-гейты: доля клиентской базы", fontsize=16, pad=14)
    ax.set_xlabel("Доля, %")
    ax.set_ylabel("")

    for idx, row in gates.reset_index(drop=True).iterrows():
        ax.text(row["share_total_pct"], idx, f" {row['share_total_pct']:.2f}%", va="center", ha="left", fontsize=10)

    _save_fig(fig, out_dir / "03_hybrid_gate_share.png")
    return {str(r["business_gate"]): float(r["share_total_pct"]) for _, r in gates.iterrows()}


def _ml_eligibility_chart(seg_dir: Path, out_dir: Path) -> dict[str, float]:
    gates = _safe_read_csv(seg_dir / "segment_hybrid_gate_profile.csv")
    if gates.empty:
        return {}

    gates = gates.copy()
    gates["gate_pretty"] = gates["business_gate"].map(_clean_gate_name)
    gates["ineligible_ls_count"] = gates["ls_count"] - gates["ml_eligible_ls_count"]

    plot_df = gates[["gate_pretty", "ml_eligible_ls_count", "ineligible_ls_count"]].melt(
        id_vars=["gate_pretty"],
        value_vars=["ml_eligible_ls_count", "ineligible_ls_count"],
        var_name="status",
        value_name="ls_count",
    )
    status_map = {
        "ml_eligible_ls_count": "ML eligible",
        "ineligible_ls_count": "Rule-only",
    }
    plot_df["status"] = plot_df["status"].map(status_map)

    fig, ax = plt.subplots(figsize=(11.5, 6.2))
    sns.barplot(data=plot_df, x="gate_pretty", y="ls_count", hue="status", palette=["#2E8B57", "#B22222"], ax=ax)
    ax.set_title("ML-допустимость внутри бизнес-гейтов", fontsize=16, pad=14)
    ax.set_xlabel("")
    ax.set_ylabel("Количество ЛС")
    ax.tick_params(axis="x", rotation=25)
    ax.legend(title="Маршрут")

    _save_fig(fig, out_dir / "04_ml_eligibility_by_gate.png")
    return {
        "ml_eligible_total": float(gates["ml_eligible_ls_count"].sum()),
        "ineligible_total": float(gates["ineligible_ls_count"].sum()),
    }


def _hybrid_cluster_mix_chart(seg_dir: Path, out_dir: Path) -> None:
    clusters = _safe_read_csv(seg_dir / "segment_hybrid_cluster_profile.csv")
    if clusters.empty:
        return

    clusters = clusters.copy()
    clusters["gate_pretty"] = clusters["business_gate"].map(_clean_gate_name)

    pivot = clusters.pivot_table(
        index="gate_pretty",
        columns="hybrid_cluster_id",
        values="share_within_gate_pct",
        aggfunc="sum",
        fill_value=0.0,
    )

    fig, ax = plt.subplots(figsize=(10.8, 6.2))
    sns.heatmap(pivot, annot=True, fmt=".1f", cmap="YlGnBu", linewidths=0.5, cbar_kws={"label": "% внутри gate"}, ax=ax)
    ax.set_title("Внутренняя структура ML-кластеров по gate", fontsize=16, pad=14)
    ax.set_xlabel("Hybrid cluster id")
    ax.set_ylabel("")

    _save_fig(fig, out_dir / "05_hybrid_cluster_heatmap.png")


def _uplift_summary_chart(uplift_dir: Path, out_dir: Path) -> dict[str, float]:
    summary = _safe_read_csv(uplift_dir / "uplift_measure_summary.csv")
    if summary.empty:
        return {}

    fig, ax = plt.subplots(figsize=(10.8, 6.1))
    plot_df = summary[["measure", "naive_ate", "ate_dr"]].melt(
        id_vars=["measure"],
        value_vars=["naive_ate", "ate_dr"],
        var_name="metric",
        value_name="value",
    )
    metric_map = {"naive_ate": "Naive ATE", "ate_dr": "DR ATE"}
    plot_df["metric"] = plot_df["metric"].map(metric_map)

    sns.barplot(data=plot_df, x="measure", y="value", hue="metric", palette=["#4682B4", "#FF8C00"], ax=ax)
    ax.axhline(0.0, color="black", linewidth=1)
    ax.set_title("Оценка эффекта мер: наивная и DR-оценка", fontsize=16, pad=14)
    ax.set_xlabel("Мера")
    ax.set_ylabel("Эффект (uplift) на paid_any_next_month")
    ax.legend(title="Оценка")

    _save_fig(fig, out_dir / "06_uplift_effect_estimates.png")
    return {str(r["measure"]): float(r["ate_dr"]) for _, r in summary.iterrows()}


def _recommendation_mix_chart(uplift_dir: Path, out_dir: Path, score_month: date | None) -> dict[str, int]:
    recs = _safe_read_csv(uplift_dir / "uplift_recommendations.csv")
    if recs.empty:
        return {}

    if score_month is not None and "month" in recs.columns:
        recs = recs[recs["month"] == score_month.isoformat()].copy()

    recs = recs.copy()
    recs["recommended_measure"] = recs["recommended_measure"].fillna("").replace("", "no_action")
    agg = recs.groupby("recommended_measure", as_index=False).size().sort_values("size", ascending=False)

    fig, ax = plt.subplots(figsize=(9.8, 5.8))
    sns.barplot(
        data=agg,
        x="recommended_measure",
        y="size",
        hue="recommended_measure",
        palette="rocket",
        dodge=False,
        legend=False,
        ax=ax,
    )
    ax.set_title("Распределение рекомендуемых мер", fontsize=16, pad=14)
    ax.set_xlabel("Рекомендация")
    ax.set_ylabel("Количество ЛС")
    ax.tick_params(axis="x", rotation=20)

    for i, v in enumerate(agg["size"]):
        ax.text(i, v, f"{int(v):,}".replace(",", " "), ha="center", va="bottom", fontsize=10)

    _save_fig(fig, out_dir / "07_recommended_measure_mix.png")
    return {str(r["recommended_measure"]): int(r["size"]) for _, r in agg.iterrows()}


def _recommendation_reason_chart(uplift_dir: Path, out_dir: Path, score_month: date | None) -> dict[str, int]:
    recs = _safe_read_csv(uplift_dir / "uplift_recommendations.csv")
    if recs.empty:
        return {}

    if score_month is not None and "month" in recs.columns:
        recs = recs[recs["month"] == score_month.isoformat()].copy()

    agg = recs.groupby("recommendation_reason", as_index=False).size().sort_values("size", ascending=False)

    fig, ax = plt.subplots(figsize=(9.8, 5.8))
    sns.barplot(
        data=agg,
        x="recommendation_reason",
        y="size",
        hue="recommendation_reason",
        palette="flare",
        dodge=False,
        legend=False,
        ax=ax,
    )
    ax.set_title("Причины выбора/отклонения меры", fontsize=16, pad=14)
    ax.set_xlabel("Причина")
    ax.set_ylabel("Количество ЛС")
    ax.tick_params(axis="x", rotation=20)

    for i, v in enumerate(agg["size"]):
        ax.text(i, v, f"{int(v):,}".replace(",", " "), ha="center", va="bottom", fontsize=10)

    _save_fig(fig, out_dir / "08_recommendation_reason_mix.png")
    return {str(r["recommendation_reason"]): int(r["size"]) for _, r in agg.iterrows()}


def _uplift_distribution_chart(uplift_dir: Path, out_dir: Path, score_month: date | None) -> None:
    scores = _safe_read_csv(uplift_dir / "uplift_measure_scores.csv")
    if scores.empty:
        return

    if score_month is not None and "month" in scores.columns:
        scores = scores[scores["month"] == score_month.isoformat()].copy()

    if "gate_ml_eligible" in scores.columns:
        scores = scores[scores["gate_ml_eligible"].astype(str) != "0"]

    if scores.empty:
        return

    # Downsample for rendering speed
    if len(scores) > 120000:
        scores = scores.sample(120000, random_state=42)

    fig, ax = plt.subplots(figsize=(11.2, 6.1))
    sns.violinplot(
        data=scores,
        x="measure",
        y="uplift_score",
        hue="measure",
        dodge=False,
        legend=False,
        inner="quartile",
        cut=0,
        palette="coolwarm",
        ax=ax,
    )
    ax.axhline(0.0, color="black", linewidth=1)
    ax.set_title("Распределение uplift-оценок (только ML-eligible)", fontsize=16, pad=14)
    ax.set_xlabel("Мера")
    ax.set_ylabel("Uplift score")

    _save_fig(fig, out_dir / "09_uplift_distribution.png")


def _write_storyline(
    out_dir: Path,
    score_month: date | None,
    pipeline_rows: dict[str, float],
    dq_counts: dict[str, int],
    gate_share: dict[str, float],
    eligibility: dict[str, float],
    ate_dr: dict[str, float],
    rec_mix: dict[str, int],
    reason_mix: dict[str, int],
) -> None:
    lines = []
    lines.append("# Storyline For Presentation")
    lines.append("")
    lines.append(f"- Период визуализации: `{score_month.isoformat() if score_month else 'auto/latest'}`")

    if pipeline_rows:
        lines.append("- Объем слоев пайплайна:")
        for k, v in pipeline_rows.items():
            lines.append(f"  - {k}: {int(v):,}".replace(",", " "))

    if dq_counts:
        top_dq = sorted(dq_counts.items(), key=lambda x: x[1], reverse=True)[:3]
        lines.append("- ТОП DQ-ошибок:")
        for k, v in top_dq:
            lines.append(f"  - {k}: {v:,}".replace(",", " "))

    if gate_share:
        top_gate = max(gate_share.items(), key=lambda x: x[1])
        lines.append(f"- Крупнейший business-gate: `{top_gate[0]}` ({top_gate[1]:.2f}%).")

    if eligibility:
        ml = int(eligibility.get("ml_eligible_total", 0))
        inel = int(eligibility.get("ineligible_total", 0))
        lines.append(f"- ML-eligible: {ml:,}, rule-only: {inel:,}.".replace(",", " "))

    if ate_dr:
        best_ate = max(ate_dr.items(), key=lambda x: x[1])
        lines.append(f"- Лучшая DR-оценка эффекта: `{best_ate[0]}` ({best_ate[1]:.4f}).")

    if rec_mix:
        top_rec = max(rec_mix.items(), key=lambda x: x[1])
        lines.append(f"- Самая частая рекомендация: `{top_rec[0]}` ({top_rec[1]:,} ЛС).".replace(",", " "))

    if reason_mix:
        top_reason = max(reason_mix.items(), key=lambda x: x[1])
        lines.append(f"- Доминирующая причина в рекомендациях: `{top_reason[0]}` ({top_reason[1]:,}).".replace(",", " "))

    lines.append("")
    lines.append("## Generated Charts")
    lines.append("- 01_pipeline_layers_rows.png")
    lines.append("- 02_dq_issue_distribution.png")
    lines.append("- 03_hybrid_gate_share.png")
    lines.append("- 04_ml_eligibility_by_gate.png")
    lines.append("- 05_hybrid_cluster_heatmap.png")
    lines.append("- 06_uplift_effect_estimates.png")
    lines.append("- 07_recommended_measure_mix.png")
    lines.append("- 08_recommendation_reason_mix.png")
    lines.append("- 09_uplift_distribution.png")

    (out_dir / "presentation_storyline.md").write_text("\n".join(lines), encoding="utf-8")


def run(input_dir: Path, out_dir: Path, score_month: str | None, dpi: int) -> dict[str, object]:
    global DEFAULT_DPI
    DEFAULT_DPI = dpi

    sns.set_theme(style="whitegrid", context="talk")

    if score_month:
        target_month = parse_as_of_month(score_month)
    else:
        target_month = None

    ensure_dir(out_dir)

    normalized_dir = _resolve_layer(input_dir, "normalized", "dq_issues.csv")
    segmentation_dir = _resolve_layer(input_dir, "segmentation", "segment_hybrid_gate_profile.csv")
    uplift_dir = _resolve_layer(input_dir, "uplift", "uplift_recommendations.csv")

    pipeline_rows = _pipeline_overview_chart(input_dir, out_dir)
    dq_counts = _dq_chart(normalized_dir, out_dir)
    gate_share = _hybrid_gate_share_chart(segmentation_dir, out_dir)
    eligibility = _ml_eligibility_chart(segmentation_dir, out_dir)
    _hybrid_cluster_mix_chart(segmentation_dir, out_dir)
    ate_dr = _uplift_summary_chart(uplift_dir, out_dir)
    rec_mix = _recommendation_mix_chart(uplift_dir, out_dir, target_month)
    reason_mix = _recommendation_reason_chart(uplift_dir, out_dir, target_month)
    _uplift_distribution_chart(uplift_dir, out_dir, target_month)

    _write_storyline(out_dir, target_month, pipeline_rows, dq_counts, gate_share, eligibility, ate_dr, rec_mix, reason_mix)

    charts = sorted([p.name for p in out_dir.glob("*.png")])
    metrics = {
        "stage": "build_presentation_viz",
        "input_dir": str(input_dir),
        "out_dir": str(out_dir),
        "score_month": target_month.isoformat() if target_month else None,
        "charts_generated": charts,
        "charts_count": len(charts),
        "storyline": "presentation_storyline.md",
    }

    (out_dir / "load_metrics_presentation_viz.json").write_text(
        json.dumps(metrics, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return metrics


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    presentation_cfg = PIPELINE_CONFIG["presentation"]
    metrics = run(
        input_dir=Path(OUTPUT_DIR),
        out_dir=Path(PRESENTATION_DIR),
        score_month=PIPELINE_CONFIG.get("month"),
        dpi=int(presentation_cfg["dpi"]),
    )
    print(json.dumps(metrics, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
