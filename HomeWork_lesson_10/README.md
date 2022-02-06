# Домашнее задание 5

- Напишите Airflow DAG, который выгружает данные из PostgreSQL базы данных в HDFS в CSV формате.

    - **hw_l10_data_from_postgresql_dag.py** - настройка dag

    - **hw_l10_data_from_postgresql_helper.py** - логика и временно настройки для подключения к базе

---

- Также переделайте все Airflow DAG из предыдущих домашних заданий так, чтобы вместо диска, данные сохранялись в "bronze".
Airflow DAG должна будет выполняться каждый день, организуйте хранение файлов соответственно.
Хранение данных в "bronze" тоже должно иметь организацию.

    - **hw_l7_data_to_hdfs_dag.py** - настройка dag

    - **hw_l7_data_to_hdfs_from_api_helper.py** - логика и временно настройки

    - **hw_l7_data_to_hdfs_from_postgresql_helper.py** - логика и временно настройки


