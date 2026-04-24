from __future__ import annotations

from pathlib import Path


def write_data_dictionary(path: Path, include_mart: bool = True) -> None:
    content = """# Data Dictionary

## `dim_ls_profile`
- `ls_id`: Идентификатор лицевого счета (строка цифр).
- `can_remote_disconnect`: Признак возможности дистанционного отключения (0/1).
- `has_phone`: Наличие телефона (0/1).
- `has_benefits`: Наличие льгот (0/1).
- `is_gasified`: Газификация дома (0/1).
- `is_city`: Город (0/1).
- `has_yaroblierc_receipt`: Признак канала квитанции ЯрОблИЕРЦ (0/1).
- `has_post_receipt`: Признак канала квитанции Почта России (0/1).
- `has_e_receipt`: Признак электронной квитанции (0/1).
- `not_resident`: Признак "не проживает" (0/1).
- `is_chd`: Частный дом (0/1).
- `is_mkd`: МКД (0/1).
- `is_dormitory`: Общежитие (0/1).
- `meter_install_tambour`: Место установки прибора (тамбур) (0/1).
- `meter_install_pole`: Место установки прибора (опора) (0/1).
- `meter_install_apartment_house`: Место установки прибора (квартира/дом) (0/1).
- `meter_install_stairwell`: Место установки прибора (лестничная клетка) (0/1).
- `address_guid`: Валидный GUID адреса.
- `source_record_count`: Количество исходных записей, схлопнутых в `ls_id`.

## `fact_balance_monthly`
- `ls_id`: Идентификатор лицевого счета.
- `period_month`: Месяц баланса (`YYYY-MM-DD`, первый день месяца).
- `debt_start`: Сальдо задолженности на начало периода, руб.
- `charge_amt`: Начислено за период, руб.
- `paid_amt`: Оплачено за период, руб.
- `source_row_num`: Номер строки сырого источника.

## `fact_payment_txn`
- `ls_id`: Идентификатор лицевого счета.
- `payment_date`: Дата оплаты (`YYYY-MM-DD`).
- `amount_rub`: Сумма оплаты, руб. (может быть отрицательной корректировкой).
- `payment_method`: Код способа оплаты (`0..7` или `NULL`).
- `is_reversal`: Флаг корректировки (`1`, если `amount_rub < 0`).
- `source_row_num`: Номер строки сырого источника.

## `fact_action_event`
- `ls_id`: Идентификатор лицевого счета.
- `action_date`: Дата применения меры (`YYYY-MM-DD`).
- `action_type`: Канонический тип меры (`autodial`, `email`, `sms`, `operator_call`, `claim`, `field_visit`, `restriction_notice`, `restriction`, `court_order_request`, `court_order_received`).
- `event_count`: Количество событий на `ls_id + action_date + action_type`.

## `dim_action_limit`
- `action_type`: Канонический тип меры.
- `month_limit`: Месячный лимит применений (целое число, пусто для безлимита).
- `is_unlimited`: Флаг безлимита (`1`/`0`).
- `source_measure_name`: Исходное наименование меры.

## `dq_issues`
- `issue_id`: Идентификатор записи DQ.
- `stage`: Этап обработки (`normalized`).
- `source_file`: Имя исходного файла.
- `source_row_num`: Номер строки исходного файла/сырого слоя.
- `issue_type`: Тип проблемы качества.
- `issue_detail`: Описание проблемы.
- `ls_id_raw`: Исходное значение ЛС (если применимо).
"""

    if include_mart:
        content += """

## `mart_ls_month`
- `ls_id`: Идентификатор лицевого счета.
- `month`: Месяц среза `T` (`YYYY-MM-DD`, первый день месяца).
- `debt_start_t`: Сальдо на начало `T`, руб.
- `charge_amt_t`: Начислено в `T`, руб.
- `paid_amt_t`: Оплачено в `T` по оборотно-сальдовой ведомости, руб.
- `payment_txn_count_l1m/l3m/l6m`: Число транзакций оплаты в окне 1/3/6 месяцев до `T` (включая `T`).
- `payment_amount_l1m/l3m/l6m`: Сумма оплат по транзакциям в окне 1/3/6 месяцев, руб.
- `action_total_l1m/l3m/l6m`: Количество событий мер в окне 1/3/6 месяцев.
- `action_unique_types_l1m/l3m/l6m`: Число уникальных типов мер в окне 1/3/6 месяцев.
- `action_<type>_l1m/l3m/l6m`: Частота конкретного типа меры в окне.
- `contact_channels_count`: Количество доступных каналов коммуникации.
- `paid_any_next_month`: Целевая метка: была ли оплата в `T+1` (`0/1`).
- `paid_amount_next_month`: Сумма оплат в `T+1`, руб.
- `recovery_ratio_next_month`: Доля погашения в `T+1`: `paid_amount_next_month / max(debt_start_t + charge_amt_t, 0)`.
"""

    path.write_text(content, encoding="utf-8")
