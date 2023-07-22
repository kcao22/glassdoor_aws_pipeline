COPY public.fact
FROM 's3://kc-glassdoor-data-curated/curated/'
IAM_ROLE ''
REGION 'us-west-2'
IGNOREHEADER 1
DELIMITER ','
JOB CREATE daily_import AUTO ON;