from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import psycopg2
import os
from datetime import date


pg_creds = {
    'host':'127.0.0.1',
    'port':'5432',
    'database':'pagila',
    'user':'pguser',
    'password':'secret',
}
pg_url = f"jdbc:postgresql://{pg_creds['host']}/{pg_creds['database']}"
pg_properties = {'user':pg_creds['user'], 'password': pg_creds['password']}

spark = SparkSession.builder\
    .config('spark.driver.extraClassPath',
            '/home/user/shared_folder/postgresql-42.3.2.jar')\
    .master('local')\
    .appName('lesson_13')\
    .getOrCreate()


def task_1():
    """
        вывести количество фильмов в каждой категории, отсортировать по убыванию.
    """
    film_category_df = spark.read.jdbc(url=pg_url, table='film_category', properties=pg_properties)
    category_df = spark.read.jdbc(url=pg_url, table='category', properties=pg_properties)
    count_films_by_category_df = film_category_df\
        .join(category_df, film_category_df.category_id == category_df.category_id,'inner')\
        .select(category_df.name.alias('Film Category'))\
        .groupBy(F.col('Film Category'))\
        .count()\
        .sort(F.desc('count'))
    count_films_by_category_df.show()


def task_2():
    """
        вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.
    """
    pass
    rental_df = spark.read.jdbc(url=pg_url, table='rental', properties=pg_properties)
    inventory_df = spark.read.jdbc(url=pg_url, table='inventory', properties=pg_properties)
    film_actor_df = spark.read.jdbc(url=pg_url, table='film_actor', properties=pg_properties)
    actor_df = spark.read.jdbc(url=pg_url, table='actor', properties=pg_properties)

    top10_actors_df = rental_df\
        .join(inventory_df, inventory_df.inventory_id == rental_df.inventory_id, 'inner')\
        .join(film_actor_df, film_actor_df.film_id == inventory_df.film_id, 'inner')\
        .join(actor_df, actor_df.actor_id == film_actor_df.actor_id, 'inner')\
        .withColumn('Actor_', F.concat(actor_df.first_name, F.lit(' '), actor_df.last_name ))\
        .select(film_actor_df.actor_id, F.col('Actor_'))\
        .groupBy(film_actor_df.actor_id, F.col('Actor_'))\
        .count()\
        .sort(F.desc('count'))
    top10_actors_df.show()

def task_3():
    """
        вывести категорию фильмов, на которую потратили больше всего денег.
    """
    rental_df = spark.read.jdbc(url=pg_url, table='rental', properties=pg_properties)
    inventory_df = spark.read.jdbc(url=pg_url, table='inventory', properties=pg_properties)
    payment_df = spark.read.jdbc(url=pg_url, table='payment', properties=pg_properties)
    film_category_df = spark.read.jdbc(url=pg_url, table='film_category', properties=pg_properties)
    category_df = spark.read.jdbc(url=pg_url, table='category', properties=pg_properties)

    get_top_film_id = rental_df\
        .join(inventory_df, inventory_df.inventory_id == rental_df.inventory_id ,'inner')\
        .join(payment_df, [(payment_df.rental_id == rental_df.rental_id) & (payment_df.customer_id == rental_df.customer_id)] , 'inner')\
        .groupBy(inventory_df.film_id)\
        .agg(F.sum(payment_df.amount).alias('Sum_'))\
        .sort(F.desc('Sum_'))\
        .limit(1).select('film_id').collect()[0][0]

    get_top_film_category_df = film_category_df\
        .join(category_df, category_df.category_id == film_category_df.category_id  , 'inner')\
        .select(category_df.name.alias('Category'))\
        .where(film_category_df.film_id == get_top_film_id)

    get_top_film_category_df.show()
    


# вывести категорию фильмов, на которую потратили больше всего денег.
# вывести названия фильмов, которых нет в inventory.
# вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех..
# вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). Отсортировать по количеству неактивных клиентов по убыванию.
# вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city), и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”.



if __name__ == '__main__':
    #task_1()
    #task_2()
    task_3()




    