use sakila;

-- 1
select customer_id, payment_date, amount, sum(amount) over (partition by customer_id order by payment_date) as customer_running_total
from payment;

-- 2
select date(payment_date), amount, 
rank() over (partition by date(payment_date) order by amount desc) as b_rank, 
dense_rank() over (partition by date(payment_date) order by amount desc) as d_rank
from payment;

-- 3
select title, name, rental_rate,
rank() over (partition by name order by rental_rate desc) as b_rank,
dense_rank() over (partition by name order by rental_rate desc) as d_rank 
from film f
inner join film_category fc
on f.film_id = fc.film_id
inner join category c 
on fc.category_id = c.category_id;

-- 4
with film_ranking as
(select title, name, rental_rate,
rank() over (partition by name order by rental_rate) as b_rank,
dense_rank() over (partition by name order by rental_rate) as d_rank
from film f
inner join film_category fc
on f.film_id = fc.film_id
inner join category c 
on fc.category_id = c.category_id)

select * 
from (select *, row_number() over (partition by name order by rental_rate) as row_num
from film_ranking) as r
where row_num <= 5;

-- 5
select payment_id, customer_id, amount, payment_date,
lag(amount) over (partition by customer_id order by payment_date) as previous_amount,
lead(amount) over (partition by customer_id order by payment_date) as following_amount,
(amount-lag(amount) over (partition by customer_id order by payment_date)) as diff_from_prev,
(amount-lead(amount) over (partition by customer_id order by payment_date)) as diff_from_next
from payment;