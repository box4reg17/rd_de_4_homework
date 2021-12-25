-- 1. вывести количество фильмов в каждой категории, отсортировать по убыванию.
SELECT c."name" as "Film category", count(*) as "films count" -- f_c.film_id, f_c.category_id,
FROM public.film_category f_c
join category c on c.category_id = f_c.category_id
group by c."name"
order by count(*) desc;

--2. вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.
SELECT fa.actor_id, concat(a.first_name, ' ', a.last_name) as Actor, count(*)
FROM public.rental r
join inventory i on r.inventory_id = i.inventory_id
join film_actor fa on fa.film_id = i.film_id
join actor a on a.actor_id = fa.actor_id
group by fa.actor_id, concat(a.first_name, ' ', a.last_name)
order by count(*) desc
limit 10

--3. вывести категорию фильмов, на которую потратили больше всего денег.
select c."name" as Category from film_category fc
join category c on c.category_id = fc.category_id
where film_id = ( SELECT i.film_id
					FROM public.rental r
					join inventory i on r.inventory_id = i.inventory_id
					join payment p on p.rental_id  = r.rental_id and p.customer_id = r.customer_id
					group by i.film_id
					order by sum(p.amount) desc
					limit 1
					);

--4. вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.
select distinct f.title from film f
left join inventory i on i.film_id = f.film_id
where i.inventory_id is null

--5. вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”.
--Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.
select * from (SELECT concat(a.first_name, ' ', a.last_name) as Actor, count(*) as "films", dense_rank () over (order by count(*) desc)
FROM film_category fc
join film_actor fa on fa.film_id = fc.film_id
join actor a on a.actor_id = fa.actor_id
where fc.category_id = (select category_id from category c where c."name" = 'Children' limit 1)
group by fa.actor_id, concat(a.first_name, ' ', a.last_name)
) as t
where t.dense_rank <= 3
;

--6. вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1).
--Отсортировать по количеству неактивных клиентов по убыванию.
select a.city_id, city.city, c.active, count(*) from customer c
join address a on a.address_id  = c.address_id
join city on city.city_id = a.city_id
group by a.city_id, city.city, c.active
order by c.active, count(*) desc


--7. вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды
--в городах (customer.address_id в этом city), и которые начинаются на букву “a”.
--То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.