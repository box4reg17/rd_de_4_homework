
# Домашнее задание 4
# Используя Apache Airflow PythonOperator реализуйте следущий ELT процес:


## 1. Переписать Домашнее задание 1 с использованием Apache Airflow.

- **hw_l7_data_from_api.py** - настройка dag

- **hw_l7_data_from_api_helper.py** - логика и временно настройки для подключения к апи

---

## 2. Выгрузить все данные из дампа PostgreSQL на диск при помощи Apache Airflow.

- **hw_l7_data_from_postgresql.py** - настройка dag
- **hw_l7_data_from_postgresql_helper.py** - логика и временно настройки для подключения к базе

---

## Добавил еще один dag, где две задачи вместе
- **hw_l7_data_dag.py**

Насколько это хорошая идея, совмещать эти задачи?

Для первой задачи есть возможность запускать прошлим числом. 
У второй такой возможности нет.
