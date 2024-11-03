#https://www.kaggle.com/datasets/mexwell/famous-paintings

# 1. Fetch all the paintings which are not displayed on any museums
SELECT * FROM work
WHERE museum_id IS NULL;



# 2. Are there museuems without any paintings? No
SELECT m.museum_id 
FROM museum AS m
WHERE NOT EXISTS 
	(SELECT w.museum_id 
    FROM work AS w
    WHERE m.museum_id = w.museum_id);
    
    
    
# 3.How many paintings have an asking price of more than their regular price? 0
SELECT count(sale_price)
FROM product_size
WHERE sale_price > regular_price; 



# 4. Identify the paintings whose asking price is less than 50% of its regular price
select * 
	from product_size
	where sale_price < (regular_price*0.5);



# 5) Which canva size costs the most?
SELECT *
FROM canvas_size as c
LEFT JOIN product_size as p
ON c.size_id = p.size_id
ORDER BY sale_price DESC
LIMIT 1;


#6. Delete duplicate records from work, product_size, subject and image_link tables
SELECT work_id
FROM work
GROUP BY work_id
HAVING COUNT(*) >1;

delete from cte where rn > 1;
create table work_distinct like work;
INSERT INTO work_distinct
SELECT DISTINCT * FROM work;

SELECT work_id
FROM work_distinct
GROUP BY work_id
HAVING COUNT(*) >1;

create table product_size_distinct like product_size;
INSERT INTO product_size_distinct
SELECT DISTINCT * FROM product_size;

create table subject_distinct like subject;
INSERT INTO subject_distinct
SELECT DISTINCT * FROM subject;

create table image_link_distinct like image_link;
INSERT INTO image_link_distinct
SELECT DISTINCT * FROM image_link;

----
# 7 identify the museums which are open on both Sunday AND Monday. Display museum name, city
SELECT 
m.name,
m.city
FROM museum_hours mh1
JOIN museum m
ON m.museum_id=mh1.museum_id
WHERE day = 'Sunday'
and exists 
(select * from museum_hours mh2
			WHERE mh2.museum_id = mh1.museum_id
            and mh2.day ='Monday');
            
            
            
# 8 Which museum is open for the longest during a day. 
# Dispay museum name, state and hours open and which day?

SELECT * 
FROM museum_hours;

UPDATE museum_hours
SET open = STR_TO_DATE(open, '%h:%i:%p');

UPDATE museum_hours
SET close = STR_TO_DATE(close, '%h:%i:%p');

ALTER TABLE museum_hours
CHANGE COLUMN `open` `open` TIME NULL DEFAULT NULL ;

ALTER TABLE museum_hours
CHANGE COLUMN `close` `close` TIME NULL DEFAULT NULL ;

SELECT m.name, m.state, mh.open, mh.close, mh.day, TIMEDIFF(close, open) AS hours_open
FROM museum_hours mh
JOIN museum m
ON mh.museum_id = m.museum_id
ORDER BY hours_open DESC
LIMIT 1;


#9 Display the country and the city with most no of museums. Output 2 seperate
# columns to mention the city and country. If there are multiple value, seperate them
# with comma.

WITH cte_country as
	(SELECT COUNT(country), country,
    RANK() over(order by count(1) desc) as rnk
	FROM museum
	GROUP BY country),
    cte_city AS
	(SELECT COUNT(city), city,
    RANK() over(order by count(1) desc) as rnk
	FROM museum
	GROUP BY city)
SELECT group_concat(distinct country, ', ') as country,
group_concat( city, ', ') as city
FROM cte_country
CROSS JOIN cte_city
WHERE cte_country.rnk = 1
AND cte_city.rnk = 1;
