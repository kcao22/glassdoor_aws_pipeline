-- Dimension Tables / Lookup Tables
CREATE TABLE dimCities (
    citykey SMALLINT NOT NULL PRIMARY KEY
    , cityname VARCHAR(16)
    , stateabbrev VARCHAR(15)
    , statename VARCHAR(20)
    , statefips SMALLINT
    , lat DOUBLE PRECISION
    , lon DOUBLE PRECISION
    , city_pop2019 BIGINT
);

CREATE TABLE dimStates (
    statekey SMALLINT NOT NULL PRIMARY KEY
    , statename VARCHAR(20)
    , stateabbrev VARCHAR(15)
    , state_pop2019 BIGINT
);

CREATE TABLE dimSize (
    sizekey SMALLINT NOT NULL PRIMARY KEY
    , size VARCHAR(23)
);

CREATE TABLE dimType(
    typekey SMALLINT NOT NULL PRIMARY KEY
    , type VARCHAR(15)
);

CREATE TABLE dimRevenue(
    revenuekey SMALLINT NOT NULL PRIMARY KEY
    , revenue VARCHAR(32)
);

CREATE TABLE dimSector(
    sectorkey SMALLINT NOT NULL PRIMARY KEY
    , sector VARCHAR(32)
);

CREATE TABLE dimIndustry(
    industrykey SMALLINT NOT NULL PRIMARY KEY 
    , industry VARCHAR(40)
);

CREATE TABLE dimEducation (
    educationkey SMALLINT NOT NULL PRIMARY KEY 
    , education VARCHAR(15)
);

CREATE TABLE dimExperience (
    experiencekey SMALLINT NOT NULL PRIMARY KEY
    , experience VARCHAR(15)
);

-- Fact table
CREATE TABLE fact (
    date VARCHAR(30) 
    , companyname VARCHAR(MAX)
    , companyrating DOUBLE PRECISION
    , revenuekey SMALLINT REFERENCES dimRevenue(revenuekey)
    , sectorkey SMALLINT REFERENCES dimSector(sectorkey)
    , industrykey SMALLINT REFERENCES dimIndustry(industrykey)
    , sizekey SMALLINT REFERENCES dimSize(sizekey)
    , typekey SMALLINT REFERENCES dimType(typekey)
    , educationkey SMALLINT REFERENCES dimEducation(educationkey)
    , experiencekey SMALLINT REFERENCES dimExperience(experiencekey)
    , companyyearfounded SMALLINT
    , companyage SMALLINT
    , easyapply VARCHAR(MAX)
    , citykey SMALLINT REFERENCES dimCities(citykey)
    , statekey SMALLINT REFERENCES dimStates(statekey)
    , minsalary DOUBLE PRECISION
    , maxsalary DOUBLE PRECISION
    , averagesalary DOUBLE PRECISION
    , stream SMALLINT
    , sql SMALLINT
    , gcp SMALLINT
    , scala SMALLINT
    , dbt SMALLINT
    , java  SMALLINT
    , azure SMALLINT
    , aws SMALLINT
    , batch SMALLINT
    , spark SMALLINT
    , python SMALLINT
    , airflow SMALLINT
);