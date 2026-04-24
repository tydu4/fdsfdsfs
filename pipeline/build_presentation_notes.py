from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

from .common import ensure_dir
from .settings import OUTPUT_DIR, PRESENTATION_DIR


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _to_float(raw: str | None) -> float:
    try:
        return float(str(raw or "0").strip() or 0)
    except ValueError:
        return 0.0


def _to_int(raw: str | None) -> int:
    try:
        return int(float(str(raw or "0").strip() or 0))
    except ValueError:
        return 0


def _fmt_int(x: int | float) -> str:
    return f"{int(x):,}".replace(",", " ")


def _clean_gate(raw: str) -> str:
    x = str(raw or "").strip()
    return x.replace("gate_", "")


def build_notes(root: Path, presentation_dir: Path) -> dict[str, object]:

    seg_metrics = _read_json(root / "segmentation" / "load_metrics_segmentation.json")
    uplift_metrics = _read_json(root / "uplift" / "load_metrics_uplift.json")

    gate_profile = _read_csv(root / "segmentation" / "segment_hybrid_gate_profile.csv")
    uplift_summary = _read_csv(root / "uplift" / "uplift_measure_summary.csv")
    uplift_recs = _read_csv(root / "uplift" / "uplift_recommendations.csv")

    rows_segmented = _to_int(seg_metrics.get("rows_segmented"))
    hybrid_meta = seg_metrics.get("hybrid_meta", {}) or {}
    ml_eligible = _to_int(hybrid_meta.get("hybrid_ml_eligible_rows"))
    ml_ineligible = _to_int(hybrid_meta.get("hybrid_ml_ineligible_rows"))

    top_gate = "n/a"
    top_gate_share = 0.0
    if gate_profile:
        top = max(gate_profile, key=lambda r: _to_float(r.get("share_total_pct")))
        top_gate = _clean_gate(top.get("business_gate", ""))
        top_gate_share = _to_float(top.get("share_total_pct"))

    best_measure = "n/a"
    best_ate_dr = 0.0
    if uplift_summary:
        top = max(uplift_summary, key=lambda r: _to_float(r.get("ate_dr")))
        best_measure = top.get("measure", "n/a")
        best_ate_dr = _to_float(top.get("ate_dr"))

    rec_mix: dict[str, int] = {}
    reason_mix: dict[str, int] = {}
    for row in uplift_recs:
        m = (row.get("recommended_measure", "") or "").strip() or "no_action"
        rec_mix[m] = rec_mix.get(m, 0) + 1
        rsn = (row.get("recommendation_reason", "") or "").strip() or "unknown"
        reason_mix[rsn] = reason_mix.get(rsn, 0) + 1

    top_rec_measure = "n/a"
    top_rec_count = 0
    if rec_mix:
        top_rec_measure, top_rec_count = max(rec_mix.items(), key=lambda x: x[1])

    top_reason = "n/a"
    top_reason_count = 0
    if reason_mix:
        top_reason, top_reason_count = max(reason_mix.items(), key=lambda x: x[1])

    score_month = uplift_metrics.get("score_month", seg_metrics.get("target_month", "n/a"))

    deck_outline = f"""# Deck Outline (10 Slides)

## Slide 1. Титул
- Заголовок: "Интеллектуальная система взыскания: от правил к персональным действиям"
- Подзаголовок: период расчета `{score_month}`
- Роль: коротко обозначить проблему и ценность для бизнеса.

## Slide 2. Бизнес-проблема и цель
- Проблема: кассовые разрывы из-за просроченной ДЗ.
- Цель: персонализированный план воздействия при ресурсных ограничениях.
- KPI-ориентир: рост доли оплат и ускорение погашения.

## Slide 3. Данные и качество (графики 01 + 02)
- Визуалы: `01_pipeline_layers_rows.png`, `02_dq_issue_distribution.png`.
- Ключевая цифра: обработано { _fmt_int(rows_segmented) } ЛС на целевой месяц.
- Пояснение: контроль DQ встроен в процесс, аномалии изолируются.

## Slide 4. Гибридная логика сегментации (графики 03 + 04)
- Визуалы: `03_hybrid_gate_share.png`, `04_ml_eligibility_by_gate.png`.
- Ключевая цифра: ML-eligible = { _fmt_int(ml_eligible) }, rule-only = { _fmt_int(ml_ineligible) }.
- Ключевой gate: `{top_gate}` ({top_gate_share:.2f}%).

## Slide 5. Внутренняя структура сегментов (график 05)
- Визуал: `05_hybrid_cluster_heatmap.png`.
- Сообщение: внутри каждого gate выделяются поведенческие подтипы для более точного действия.

## Slide 6. Эффект мер (графики 06 + 09)
- Визуалы: `06_uplift_effect_estimates.png`, `09_uplift_distribution.png`.
- Лучшая DR-оценка в прототипе: `{best_measure}` = {best_ate_dr:.4f}.
- Сообщение: решение опирается на ожидаемый эффект, а не только на фиксированные правила.

## Slide 7. Рекомендации системе (графики 07 + 08)
- Визуалы: `07_recommended_measure_mix.png`, `08_recommendation_reason_mix.png`.
- Самая частая рекомендация: `{top_rec_measure}` ({_fmt_int(top_rec_count)} ЛС).
- Главная причина маршрутизации: `{top_reason}` ({_fmt_int(top_reason_count)}).

## Slide 8. Примеры бизнес-сценариев
- 2-3 кейса: ML-eligible с положительным uplift, ML-eligible с non-positive uplift, rule-only gate.
- Объяснить, почему система выбрала/не выбрала меру.

## Slide 9. Ожидаемый эффект и внедрение
- Пилот: 1-2 месяца, A/B в части портфеля.
- Метрики: paid_any_next_month, сумма погашений, cost-to-collect.
- Операционные ограничения: лимиты мер, юридическая допустимость.

## Slide 10. Дорожная карта
- Этап 1: baseline + hybrid (готово).
- Этап 2: uplift-калибровка и мониторинг дрейфа.
- Этап 3: оптимизатор портфеля с учетом лимитов/стоимости.
"""

    speaker_notes = f"""# Speaker Notes (RU)

## Slide 1
"Мы построили не просто модель, а управляемый контур принятия решений для взыскания. Цель — уменьшить кассовый разрыв, сохранив прозрачность и управляемость бизнесом."

## Slide 2
"До проекта стратегия была в основном rule-based. Мы сохранили обязательные бизнес-ограничения и добавили слой интеллектуального выбора там, где это допустимо и дает эффект."

## Slide 3
"По данным мы прошли полный путь от raw до витрины. На целевой месяц обработано { _fmt_int(rows_segmented) } лицевых счетов. DQ-контроль встроен: проблемные записи не ломают модель, а аккуратно маршрутизируются."

## Slide 4
"Это ключевая идея гибрида. Сначала жесткие гейты, затем ML только в допустимых группах. Сейчас ML-eligible — { _fmt_int(ml_eligible) }, а rule-only — { _fmt_int(ml_ineligible) }. Самый большой gate — {top_gate}."

## Slide 5
"Внутри gate клиенты неоднородны, поэтому мы дополнительно делим их на подтипы. Это позволяет назначать действия точнее и экономить лимитированные ресурсы мер."

## Slide 6
"Здесь показан эффект мер в uplift-подходе. В нашем прототипе лучшая DR-оценка — {best_measure}: {best_ate_dr:.4f}. Мы используем именно ожидаемый прирост эффекта, а не сырой historical rate."

## Slide 7
"Система выдает конкретную рекомендацию на ЛС. Самая частая рекомендация сейчас — {top_rec_measure}. Важно: значимая доля клиентов корректно остается без ML-рекомендации из-за gate-ограничений."

## Slide 8
"На примерах видно три типа решений: 1) positive uplift и назначение меры; 2) non-positive uplift и осознанный no-action; 3) rule-only gate, где соблюдаем бизнес-допустимость."

## Slide 9
"Для внедрения предлагаем пилот с A/B-дизайном. Смотрим не только на факт оплаты, но и на стоимость взыскания и скорость возврата денег, чтобы подтвердить экономический эффект."

## Slide 10
"Технический фундамент уже готов: pipeline, сегментация, гибридные правила и uplift-прототип. Следующий шаг — доведение до production-контура с регулярным мониторингом и портфельной оптимизацией."
"""

    checklist = """# Presentation Checklist

- [ ] Проверить, что в слайдах указан период `score_month`.
- [ ] На слайде 4 проговорить, почему часть клиентов rule-only.
- [ ] На слайде 6 отдельно объяснить разницу naive ATE vs DR ATE.
- [ ] На слайде 7 показать, что `no_action` — осознанное решение, а не ошибка.
- [ ] Подготовить 2-3 кейса из `uplift_recommendations.csv` для слайда 8.
- [ ] В финале согласовать pilot KPI и срок пилота.
"""

    (presentation_dir / "deck_outline.md").write_text(deck_outline, encoding="utf-8")
    (presentation_dir / "speaker_notes.md").write_text(speaker_notes, encoding="utf-8")
    (presentation_dir / "presentation_checklist.md").write_text(checklist, encoding="utf-8")

    return {
        "rows_segmented": rows_segmented,
        "ml_eligible": ml_eligible,
        "ml_ineligible": ml_ineligible,
        "top_gate": top_gate,
        "top_gate_share": top_gate_share,
        "best_measure": best_measure,
        "best_ate_dr": best_ate_dr,
        "top_recommended_measure": top_rec_measure,
        "top_recommended_count": top_rec_count,
        "top_reason": top_reason,
        "top_reason_count": top_reason_count,
    }


def run(input_dir: Path, out_dir: Path) -> dict[str, object]:
    ensure_dir(out_dir)
    summary = build_notes(input_dir, out_dir)

    metrics = {
        "stage": "build_presentation_notes",
        "input_dir": str(input_dir),
        "out_dir": str(out_dir),
        "outputs": ["deck_outline.md", "speaker_notes.md", "presentation_checklist.md"],
        "summary": summary,
    }

    (out_dir / "load_metrics_presentation_notes.json").write_text(
        json.dumps(metrics, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return metrics


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    metrics = run(Path(OUTPUT_DIR), Path(PRESENTATION_DIR))
    print(json.dumps(metrics, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
