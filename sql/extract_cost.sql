SELECT project.name AS project_name, service.description AS service_name, sum(cost) AS total_cost
FROM {{ bq_table }} 
WHERE cast(export_time as date) = '{{ extract_date(ti) }}'
group by project_name, service_name