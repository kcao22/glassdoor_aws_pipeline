--Proc v1
CREATE OR REPLACE PROCEDURE insert_dimension(uri VARCHAR(MAX), tablename VARCHAR(MAX))
LANGUAGE PLPGSQL
AS $$
BEGIN
    EXECUTE FORMAT(
        '
            COPY %I
            FROM ''%I''
            IAM_ROLE ''arn:aws:iam::288113613717:role/RedshiftDBAdminRole''
            IGNOREHEADER 1
            DELIMITER '',''
            REGION ''us-west-2''
        '
        , tablename
        , uri
    );
END;
$$;

--Proc v2
DROP FUNCTION my_procedure
CREATE OR REPLACE PROCEDURE my_procedure(tablename VARCHAR(MAX), uri VARCHAR(MAX))
LANGUAGE plpgsql
AS $$
DECLARE
    dynamicsql text;
BEGIN
    dynamicsql := '
            COPY ' || tablename || '
            FROM ''' || uri || '''
            IAM_ROLE ''arn:aws:iam::288113613717:role/RedshiftDBAdminRole''
            IGNOREHEADER 1
            DELIMITER '',''
            REGION ''us-west-2''
        ';
    EXECUTE REPLACE(REPLACE(dynamicsql, 'tablename', tablename), 'uri', uri);
END;
$$;

-- Manually loading data if needed
-- Load cities data
COPY public.dimcities
FROM 's3://kc-glassdoor-data-curated/dimTables/dimCities/'
IAM_ROLE 'arn:aws:iam::288113613717:role/RedshiftDBAdminRole'
IGNOREHEADER 1
DELIMITER ','
REGION 'us-west-2'

--Load sates data
COPY public.dimstates
FROM 's3://kc-glassdoor-data-curated/dimTables/dimStates/'
IAM_ROLE 'arn:aws:iam::288113613717:role/RedshiftDBAdminRole'
IGNOREHEADER 1
DELIMITER ','
REGION 'us-west-2'

--Load education data
COPY public.dimeducation
FROM 's3://kc-glassdoor-data-curated/dimTables/dimEducation/'
IAM_ROLE 'arn:aws:iam::288113613717:role/RedshiftDBAdminRole'
IGNOREHEADER 1
DELIMITER ','
REGION 'us-west-2'

--Load experience data
COPY public.dimexperience
FROM 's3://kc-glassdoor-data-curated/dimTables/dimExperience/'
IAM_ROLE 'arn:aws:iam::288113613717:role/RedshiftDBAdminRole'
IGNOREHEADER 1
DELIMITER ','
REGION 'us-west-2'

--Load industry data
COPY public.dimindustry
FROM 's3://kc-glassdoor-data-curated/dimTables/dimIndustry/'
IAM_ROLE 'arn:aws:iam::288113613717:role/RedshiftDBAdminRole'
IGNOREHEADER 1
DELIMITER ','
REGION 'us-west-2'

--Load revenue data
COPY public.dimrevenue
FROM 's3://kc-glassdoor-data-curated/dimTables/dimRevenue/'
IAM_ROLE 'arn:aws:iam::288113613717:role/RedshiftDBAdminRole'
IGNOREHEADER 1
DELIMITER ','
REGION 'us-west-2'

--Load sector data
COPY public.dimsector
FROM 's3://kc-glassdoor-data-curated/dimTables/dimSector/'
IAM_ROLE 'arn:aws:iam::288113613717:role/RedshiftDBAdminRole'
IGNOREHEADER 1
DELIMITER ','
REGION 'us-west-2'

--Load size data
COPY public.dimsize
FROM 's3://kc-glassdoor-data-curated/dimTables/dimSize/'
IAM_ROLE 'arn:aws:iam::288113613717:role/RedshiftDBAdminRole'
IGNOREHEADER 1
DELIMITER ','
REGION 'us-west-2'


--Load type data
COPY public.dimtype
FROM 's3://kc-glassdoor-data-curated/dimTables/dimType/'
IAM_ROLE 'arn:aws:iam::288113613717:role/RedshiftDBAdminRole'
IGNOREHEADER 1
DELIMITER ','
REGION 'us-west-2'


--Load to fact
COPY public.fact
FROM 's3://kc-glassdoor-data-curated/curated/curated_20230718/run-1689735427297-part-block-0-r-00000-snappy.parquet'
IAM_ROLE 'arn:aws:iam::288113613717:role/RedshiftDBAdminRole'
REGION 'us-west-2'
FORMAT AS PARQUET

-- Error handling / logs
SELECT *
FROM sys_load_error_detail

SELECT * FROM svl_spectrum_scan_error
