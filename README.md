# MLOps-hw3

## Описание

Сервис скоринга читает транзакции из Kafka, считает `score`, `fraud_flag` и пишет их в таблицу `scores` вместе с `model_version`. Модель берется из общего каталога `/opt/airflow/models` и подхватывается без перезапуска

## Шаг 0
Создать `.env` и указать в `TRAIN_DATA_CSV` полный путь к `train.csv` (не загружается на гх из-за размера)
```bash
cp .env.example .env
```

## Шаг 1: докер
```bash
docker compose up --build -d
```
Airflow UI: http://localhost:8081 (`admin` / `admin`). Все DAG запускаются раз в 5 минут, вручную - открыть DAG и нажать run справа сверху.

## Шаг 2: само задание
### 1. Препроцессинг
`fraud_detector/src/preprocessing.py`, общий для обучения и скоринга:
* календарные признаки: `year`, `month`, `day`, `hour`, `minute`, `weekend`, `day_of_week`, `is_night` (22:00-6:00)
* `log_amount`, `log_population` - логарифм суммы и населения города
* `haversine` - расстояние между клиентом и продавцом
* `is_online` - онлайн-категория (`cat_id` оканчивается на `_net`)

### 2. Поставка данных: DAG `supply_labeled_batch`
Берем **50 000** случайных строк витрины `labeled_transactions`, переносим `transaction_time` в следующий месяц, дописываем батч и удаляем наблюдения старше **2 лет** от самого нового месяца

### 3. Переобучение: DAG `retrain_catboost`
`extract_month_batch -> slide_training_window -> time_train_test_split -> preprocess_dataset -> fit_catboost -> compare_with_prod -> publish_model`

Окно обучения - 3 месяца: 2 ранних в train, самый свежий в test. Указатель месяца обновляется только в `publish_model`, поэтому повторный запуск после ошибки не пропускает месяц

Проверяем, что появилась новая модель и `latest.txt` указывает на нее:
```bash
docker compose exec airflow ls -la /opt/airflow/models
docker compose exec airflow cat /opt/airflow/models/latest.txt
```
Проверяем, что новые транзакции скорятся этой версией (у самых свежих записей версия из `latest.txt`, до первого переобучения - `initial`):
```bash
docker compose exec postgres psql -U fraud_user -d fraud_db -c "SELECT model_version, count(*), max(created_at) AS last_score FROM scores GROUP BY model_version ORDER BY last_score DESC;"
```

### 4. Проверка улучшения модели
Задача `compare_with_prod`:
* новая и текущая прод-модели считаются на одном и том же месяце из test, который не видела ни одна из них
* метрика - ROC-AUC, если в test один класс - F1
* если новая модель не хуже прода - публикуем и обновляем `latest.txt`, иначе не бубликуем
* стартовая модель `initial` заменяется без сравнения
* метрики и решение пишутся в `my_catboost_<версия>.metrics.json`
```bash
docker compose exec airflow cat /opt/airflow/models/my_catboost_<версия>.metrics.json
```

### 5. Data Quality: DAG `data_quality_check`
Каждое правило - SQL-запрос к `labeled_transactions`, считающий строки, нарушающие правило. Проверка пройдена, если их 0

| Правило | Требование |
|---|---|
| `not_null_key_columns` | нет пропусков в `transaction_time`, `amount`, `cat_id`, `us_state`, `target` |
| `positive_amount` | `amount > 0` |
| `binary_target` | `target` только 0 или 1 |
| `valid_coordinates` | широта в [-90, 90], долгота в [-180, 180] |
| `valid_gender` | `gender` равен `M` или `F` |
| `history_depth` | нет наблюдений старше 2 лет от самого нового месяца |

HTML-отчет сохраняется в `/opt/airflow/dq_reports` (рядом с каталогом моделей), локально - папка `dq_reports/`:
```bash
open dq_reports/<имя файла>.html
```
Пример:
<img width="1568" height="770" alt="image" src="https://github.com/user-attachments/assets/d671f3a2-885c-4927-ad03-c1c4f460ca41" />
