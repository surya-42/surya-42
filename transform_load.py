import matplotlib.pyplot as plt
import pandas as pd
import json
import urllib.request
import xml.etree.ElementTree as ET
import plotly.graph_objects as go
import plotly.io as pio
import psycopg2


from datetime import datetime
from pymongo import MongoClient

#Extracion
#Initialize the Mongo DB connection from Source API 
#Connecting to MongoDB
#Logging using the user name and password
mongo_client = MongoClient("mongodb://localhost:27017/",username='dap',password='dap')
#Refering to the prpject created in MongoDB
project_db = mongo_client["projectdb"]

# connnection to postgress
conn = psycopg2.connect(database="postgres",
                        host="localhost",
                        user="dap",
                        password="dap",
                        port="5432")

cursor = conn.cursor()

sql_stmt4="""
DROP 
  TABLE IF EXISTS public.DIM_COVID19_VACCN_DTL;
create table public.DIM_COVID19_VACCN_DTL as 
--Dimension 1
select 
Recorded_DT , 
mmwr_week,
recip_county,
state_name,
cast(completeness_pct as NUMERIC(18,2)),
cast(administered_dose1_recip as integer),
cast(series_complete_yes_stg1 as integer),
cast(booster_doses as integer),
cast(booster_doses_vax_pct as NUMERIC(18,2)),
cast(booster_doses_vax_pct_svi as NUMERIC(18,2)),
cast(census2019 as integer),
cast(series_complete_yes_stg2 as integer),
svi_ctgy,
metro_status,
demographic_category,
cast(administered_dose1 as integer),
cast(administered_dose1_pct as NUMERIC(18,2)),
cast(series_complete_pop_pct as NUMERIC(18,2)),
cast(booster_doses_vax_pct_agegroup as NUMERIC(18,2)),
cast(booster_doses_yes as integer),
cast(second_booster_vax_pct_agegroup as NUMERIC(18,2)),
cast(second_booster as integer),
cast(bivalent_booster as integer),
now() as etl_start_dt,
NULL as etl_end_dt
from
  (
    select  
      TO_DATE(STG1.date, 'MM/DD/YYYY') as Recorded_DT,
      STG1.mmwr_week,
      STG1.recip_county,
      state.state_name,
      CASE WHEN (STG1.completeness_pct) IS NULL THEN '0' ELSE (STG1.completeness_pct) end as completeness_pct,
      CASE WHEN (STG1.administered_dose1_recip) IS NULL THEN '0' ELSE (STG1.administered_dose1_recip) end as administered_dose1_recip,
      CASE WHEN (STG1.series_complete_yes) IS NULL THEN '0' ELSE (STG1.series_complete_yes) end as series_complete_yes_stg1,
      CASE WHEN (STG1.booster_doses) IS NULL THEN '0' ELSE (STG1.booster_doses) end as booster_doses,
      CASE WHEN (STG1.booster_doses_vax_pct) IS NULL THEN '0' ELSE (STG1.booster_doses_vax_pct) end as booster_doses_vax_pct,
      CASE WHEN (STG1.booster_doses_vax_pct_svi) IS NULL THEN '0' ELSE (STG1.booster_doses_vax_pct_svi) end as booster_doses_vax_pct_svi,
      CASE WHEN (STG1.census2019) IS NULL THEN '0' ELSE (STG1.census2019) end as census2019,
      CASE WHEN (STG1.Bivalent_Booster_5Plus) IS NULL THEN '0' ELSE (STG1.Bivalent_Booster_5Plus) end as Bivalent_Booster_5Plus,
      STG1.svi_ctgy,
      STG1.metro_status,
      STG2.demographic_category,
      CASE WHEN (STG2.administered_dose1) IS NULL THEN '0' ELSE (STG2.administered_dose1) end as administered_dose1,
      CASE WHEN (STG2.series_complete_yes) IS NULL THEN '0' ELSE (STG2.series_complete_yes) end as series_complete_yes_stg2,
      CASE WHEN (STG2.administered_dose1_pct) IS NULL THEN '0' ELSE (STG2.administered_dose1_pct) end as administered_dose1_pct,
      CASE WHEN (STG2.series_complete_pop_pct) IS NULL THEN '0' ELSE (STG2.series_complete_pop_pct) end as series_complete_pop_pct,
      CASE WHEN (STG2.booster_doses_vax_pct_agegroup) IS NULL THEN '0' ELSE (STG2.booster_doses_vax_pct_agegroup) end as booster_doses_vax_pct_agegroup,
      CASE WHEN (STG2.booster_doses_yes) IS NULL THEN '0' ELSE (STG2.booster_doses_yes) end as booster_doses_yes,
      CASE WHEN (STG2.second_booster_vax_pct_agegroup) IS NULL THEN '0' ELSE (STG2.second_booster_vax_pct_agegroup) end as second_booster_vax_pct_agegroup,
      CASE WHEN (STG2.second_booster) IS NULL THEN '0' ELSE (STG2.second_booster) end as second_booster,
      CASE WHEN (STG2.bivalent_booster) IS NULL THEN '0' ELSE (STG2.bivalent_booster) end as bivalent_booster
    from
      (
        select
          date,
          fips,
          mmwr_week,
          recip_county,
          recip_state,
          completeness_pct,
          administered_dose1_recip,
          series_complete_yes,
          booster_doses,
          booster_doses_vax_pct,
          svi_ctgy,
          metro_status,
          booster_doses_vax_pct_svi,
          census2019,
		  Bivalent_Booster_5Plus
        FROM
          public.stg_covid19_vaccn
      ) STG1
      left join (
        SELECT
          date,
          demographic_category,
          administered_dose1,
          administered_dose1_pct_known,
          administered_dose1_pct_us,
          series_complete_yes,
          administered_dose1_pct,
          series_complete_pop_pct,
          series_complete_pop_pct_known,
          series_complete_pop_pct_us,
          booster_doses_vax_pct_agegroup,
          booster_doses_pop_pct_known,
          booster_doses_vax_pct_us,
          booster_doses_pop_pct_known_last14days,
          booster_doses_yes,
          booster_doses_yes_last14days,
          second_booster_vax_pct_agegroup,
          second_booster_pop_pct_known,
          second_booster_pop_pct_us,
          second_booster_pop_pct_known_last14days,
          second_booster,
          second_booster_last14days,
          bivalent_booster,
          bivalent_booster_pop_pct_agegroup,
          bivalent_booster_pop_pct_known
        FROM
          public.stg_covid19_vaccn_dmgrphc
      ) STG2 on STG1.date = STG2.date
      left join dim_us_state state on stg1.recip_state = state.state_cd
    where
      STG1.date >= '06/01/2019'
      and STG1.date <= '12/14/2022'
      and completeness_pct <> '0'
  ) DIM1
order by
  Recorded_DT asc

"""
cursor.execute(sql_stmt4)

sql_stmt5="""
DROP 
  TABLE IF EXISTS public.DIM_US_COVID_DEATH_DTL;
CREATE TABLE public.DIM_US_COVID_DEATH_DTL 
as 
SELECT DATA_AS_OF,
	START_DATE,
	END_DATE,
	GROUPCH,
	YEAR,
	MONTH,
	CASE
					WHEN DIM3.STATE_CD IS NULL THEN 'Unknown'
					ELSE DIM3.STATE_CD
	END AS STATE_CD,
	STATE,
	CONDITION_GROUP,
	CONDITION,
	ICD10_CODES,
	AGE_GROUP,
	COVID19_DEATHS,
	NUMBER_OF_MENTIONS,
	FLAG,
	SUBMISSION_DATE,
	STATE_STG4,
	TOT_CASES,
	CONF_CASES,
	PROB_CASES,
	NEW_CASE,
	PNEW_CASE,
	TOT_DEATH,
	CONF_DEATH,
	PROB_DEATH,
	NEW_DEATH,
	PNEW_DEATH,
	CREATED_AT,
	CONSENT_CASES,
	CONSENT_DEATHS,
	now() as etl_start_dt,
	null as etl_end_dt
	FROM
	(SELECT TO_DATE(DATA_AS_OF,

										'MM/DD/YYYY') AS DATA_AS_OF,
			TO_DATE(START_DATE,

				'MM/DD/YYYY') AS START_DATE,
			TO_DATE(END_DATE,

				'MM/DD/YYYY') AS END_DATE,
			GROUPCH,
			CAST(CASE
												WHEN YEAR IS NULL THEN '9999'
												ELSE YEAR
								END AS integer) AS YEAR,
			CAST(CASE
												WHEN MONTH IS NULL THEN '99'
												ELSE MONTH
								END AS integer) AS MONTH,
			--DIM3.STATE_CD, --CASE WHEN DIM3.STATE_CD IS NULL THEN 'Unknown' ELSE DIM3.STATE_CD END AS STATE_CD,
 			 STATE,
			CONDITION_GROUP,
			CONDITION,
			ICD10_CODES,
			AGE_GROUP,
			CAST(CASE
												WHEN COVID19_DEATHS IS NULL THEN '0'
												ELSE COVID19_DEATHS
								END AS integer) AS COVID19_DEATHS,
			CAST(CASE
												WHEN NUMBER_OF_MENTIONS IS NULL THEN '0'
												ELSE NUMBER_OF_MENTIONS
								END AS integer) AS NUMBER_OF_MENTIONS,
			FLAG --select count(*)
FROM PUBLIC.STG_COVID19_DEATH) STG3
LEFT JOIN PUBLIC.DIM_US_STATE DIM3 ON UPPER(STATE) = UPPER(STATE_NAME)
LEFT JOIN
	(SELECT TO_DATE(SUBMISSION_DATE,

								'MM/DD/YYYY') AS SUBMISSION_DATE,
	CASE
					WHEN STATE = 'NYC' THEN 'NY'
					WHEN STATE IS NULL THEN 'Unknown'
					ELSE STATE
	END AS STATE_STG4,
	CAST(CASE
										WHEN TOT_CASES IS NULL THEN '0'
										ELSE TOT_CASES
						END AS INTEGER) AS TOT_CASES,
	CAST(CASE
										WHEN CONF_CASES IS NULL THEN '0'
										ELSE CONF_CASES
						END AS INTEGER) AS CONF_CASES,
	CAST(CASE
										WHEN PROB_CASES IS NULL THEN '0'
										ELSE PROB_CASES
						END AS INTEGER) AS PROB_CASES,
	CAST(CASE
										WHEN NEW_CASE IS NULL THEN '0'
										ELSE NEW_CASE
						END AS INTEGER) AS NEW_CASE,
	CAST(CASE
										WHEN PNEW_CASE IS NULL THEN '0'
										ELSE PNEW_CASE
						END AS INTEGER) AS PNEW_CASE,
	CAST(CASE
										WHEN TOT_DEATH IS NULL THEN '0'
										ELSE TOT_DEATH
						END AS INTEGER) AS TOT_DEATH,
	CAST(CASE
										WHEN CONF_DEATH IS NULL THEN '0'
										ELSE CONF_DEATH
						END AS INTEGER) AS CONF_DEATH,
	CAST(CASE
										WHEN PROB_DEATH IS NULL THEN '0'
										ELSE PROB_DEATH
						END AS INTEGER) AS PROB_DEATH,
	CAST(CASE
										WHEN NEW_DEATH IS NULL THEN '0'
										ELSE NEW_DEATH
						END AS INTEGER) AS NEW_DEATH,
	CAST(CASE
										WHEN PNEW_DEATH IS NULL THEN '0'
										ELSE PNEW_DEATH
						END AS INTEGER) AS PNEW_DEATH,
	TO_DATE(CREATED_AT,

		'MM/DD/YYYY') AS CREATED_AT,
	CASE
										WHEN CONSENT_CASES IS NULL THEN 'Unknown'
										ELSE CONSENT_CASES
						END AS CONSENT_CASES,
	CASE
										WHEN CONSENT_DEATHS IS NULL THEN 'Unknown'
										ELSE CONSENT_DEATHS
						END AS CONSENT_DEATHS FROM PUBLIC.STG_COVID19_DEATH_COUNTS) STG4 ON STG4.STATE_STG4 = DIM3.STATE_CD
 
"""
cursor.execute(sql_stmt5)

sql_stmt6 = """ DROP 
  TABLE IF EXISTS public.DIM_US_VEHICLE_COLLSN_DTL;
create table public.DIM_US_VEHICLE_COLLSN_DTL as 
SELECT 
CAST(UNIQUE_ID as integer),
	CAST(COLLISION_ID as integer),
	TO_DATE(CRASH_DATE,'MM/DD/YYYY') as CRASH_DATE,
	CRASH_TIME,
	PERSON_ID,
	PERSON_TYPE, 
	PERSON_INJURY,
	CAST((CASE WHEN VEHICLE_ID IS NULL THEN '0' ELSE  VEHICLE_ID end ) as integer) as VEHICLE_ID,
	CAST(PERSON_AGE as integer),
	EJECTION,
	EMOTIONAL_STATUS,
	BODILY_INJURY,
	POSITION_IN_VEHICLE,
	SAFETY_EQUIPMENT,
	PED_LOCATION,
	PED_ACTION,
	COMPLAINT,
	PED_ROLE,
	CONTRIBUTING_FACTOR_1,
	CONTRIBUTING_FACTOR_2,
	PERSON_SEX,
	now() etl_start_dt,
	null etl_end_dt
FROM PUBLIC.STG_US_VHCLE_COLLSN where TO_DATE(CRASH_DATE,'MM/DD/YYYY')>='06/01/2019' AND TO_DATE(CRASH_DATE,'MM/DD/YYYY')>='12/10/2022'  ;
"""
cursor.execute(sql_stmt6)

conn.close()
