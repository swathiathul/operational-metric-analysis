use `operation_analytics`;

/*Number of jobs reviewed*/

select `ï»¿ds`,count(job_id) as jobs_per_day, sum(time_spent)/3600 as hours_spent 
from `operation_analytics`.`job_data`
where `ï»¿ds` >='01-11-20'  and `ï»¿ds` <='30-11-20'
group by `ï»¿ds`;

/*7 day rolling average of throughput*/
WITH CTE AS
 ( 
 SELECT 
 `ï»¿ds`, 
 COUNT(job_id) AS num_jobs, 
 SUM(time_spent) AS total_time 
 FROM `operation_analytics`.`job_data`
 WHERE event IN('transfer','decision') AND `ï»¿ds` BETWEEN '01-11-20' AND '30-11-20' GROUP BY `ï»¿ds`  )
 SELECT `ï»¿ds`, ROUND(1.0*
 SUM(num_jobs) OVER (ORDER BY `ï»¿ds` ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) / SUM(total_time)
 OVER (ORDER BY `ï»¿ds` ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),2) AS throughput_7d
 FROM CTE ;
 
/*percentage share of each language in the last 30 days*/
 
WITH CTE AS (
SELECT
Language,
COUNT(job_id) AS num_jobs
FROM
`operation_analytics`.`job_data`
WHERE
event IN('transfer','decision')
AND `ï»¿ds` BETWEEN '01-11-20' AND '30-11-20'
GROUP BY
language
),
total AS (
SELECT
COUNT(job_id) AS total_jobs
FROM
`operation_analytics`.`job_data`
WHERE
event IN('transfer','decision')
AND `ï»¿ds`BETWEEN '01-11-20' AND '30-11-20'
GROUP BY
language
)
SELECT
language,
ROUND(100.0*num_jobs/total_jobs,2) AS percentage
FROM
CTE
CROSS JOIN
total
ORDER BY
percentage DESC;

/*Display duplicates from the table*/
WITH CTE AS ( 
SELECT 
 *, ROW_NUMBER() OVER (PARTITION BY ï»¿ds, job_id, 
actor_id) AS rownum 
FROM 
 job_data 
) 
DELETE 
FROM 
 CTE 
WHERE 
rownum > 1;

/*Calculate the weekly user engagement?*/

SELECT 
 Extract(week from e.occurred_at), COUNT(DISTINCT e.user_id) 
AS 
 weekly_active_users 
FROM 
 events e 
WHERE 
 e.event_type = 'engagement' AND e.event_name = 'login' 
GROUP BY 1 ;

/*Calculate the user growth for product?*/
select * from users;

select Extract(Day from created_at) AS day,
COUNT(*) AS all_users,
COUNT(CASE WHEN activated_at IS NOT NULL THEN u.user_id ELSE
NULL END) AS activated_users
FROM users u
WHERE created_at >= '01-01-13'
AND created_at < '31-08-14'
GROUP BY 1
ORDER BY 1;

/* Calculate the email engagement metrics?*/

SELECT EXTRACT(week from occurred_at) AS week, 
COUNT(CASE WHEN e.action = 'sent weekly digest' THEN e.user_id ELSE NULL END) AS weekly_emails, 
COUNT(CASE WHEN e.action = 'sent reengagement email' THEN e.user_id ELSE NULL END) AS reengagement_emails, 
COUNT(CASE WHEN e.action = 'email open' THEN e.user_id ELSE NULL END) AS email_opens, 
COUNT(CASE WHEN e.action = 'email clickthrough' THEN e.user_id ELSE NULL END) AS email_clickthroughs FROM  email_events e 
GROUP BY 1;







