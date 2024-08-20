/* 
Doing all this fancy stuff because when I imported a CSV file into a DuckDB table using DLT, all my columns ended up concatenated ('_') into a single column. Here's what my current table structure looks like:
Columns in avocats.avocats_data:
['name_address_cour_appel_site_internet_numero_telephone_avocats_2002_avocats_2003_avocats_2004_avocats_2005_avocats_2006_avocats_2007_avocats_2008_avocats_2009_avocats_2010_avocats_2011_avocats_2012_avocats_2013_avocats_2014_avocats_2015_avocats_2016_avocats_2017_avocats_2018_avocats_2019_avocats_2020_avocats_2021_avocats_2022', '_dlt_load_id', '_dlt_id']

If you have a solution feel free to reach out to me !
*/
WITH split_data AS (
    SELECT
        SPLIT_PART(name_address_cour_appel_site_internet_numero_telephone_avocats_2002_avocats_2003_avocats_2004_avocats_2005_avocats_2006_avocats_2007_avocats_2008_avocats_2009_avocats_2010_avocats_2011_avocats_2012_avocats_2013_avocats_2014_avocats_2015_avocats_2016_avocats_2017_avocats_2018_avocats_2019_avocats_2020_avocats_2021_avocats_2022, ',', 1) AS name,
            CAST(SPLIT_PART(name_address_cour_appel_site_internet_numero_telephone_avocats_2002_avocats_2003_avocats_2004_avocats_2005_avocats_2006_avocats_2007_avocats_2008_avocats_2009_avocats_2010_avocats_2011_avocats_2012_avocats_2013_avocats_2014_avocats_2015_avocats_2016_avocats_2017_avocats_2018_avocats_2019_avocats_2020_avocats_2021_avocats_2022, ',', 6) AS INTEGER) AS avocats_2002,
            CAST(SPLIT_PART(name_address_cour_appel_site_internet_numero_telephone_avocats_2002_avocats_2003_avocats_2004_avocats_2005_avocats_2006_avocats_2007_avocats_2008_avocats_2009_avocats_2010_avocats_2011_avocats_2012_avocats_2013_avocats_2014_avocats_2015_avocats_2016_avocats_2017_avocats_2018_avocats_2019_avocats_2020_avocats_2021_avocats_2022, ',', 7) AS INTEGER) AS avocats_2003,
            CAST(SPLIT_PART(name_address_cour_appel_site_internet_numero_telephone_avocats_2002_avocats_2003_avocats_2004_avocats_2005_avocats_2006_avocats_2007_avocats_2008_avocats_2009_avocats_2010_avocats_2011_avocats_2012_avocats_2013_avocats_2014_avocats_2015_avocats_2016_avocats_2017_avocats_2018_avocats_2019_avocats_2020_avocats_2021_avocats_2022, ',', 8) AS INTEGER) AS avocats_2004,
            CAST(SPLIT_PART(name_address_cour_appel_site_internet_numero_telephone_avocats_2002_avocats_2003_avocats_2004_avocats_2005_avocats_2006_avocats_2007_avocats_2008_avocats_2009_avocats_2010_avocats_2011_avocats_2012_avocats_2013_avocats_2014_avocats_2015_avocats_2016_avocats_2017_avocats_2018_avocats_2019_avocats_2020_avocats_2021_avocats_2022, ',', 9) AS INTEGER) AS avocats_2005,
            CAST(SPLIT_PART(name_address_cour_appel_site_internet_numero_telephone_avocats_2002_avocats_2003_avocats_2004_avocats_2005_avocats_2006_avocats_2007_avocats_2008_avocats_2009_avocats_2010_avocats_2011_avocats_2012_avocats_2013_avocats_2014_avocats_2015_avocats_2016_avocats_2017_avocats_2018_avocats_2019_avocats_2020_avocats_2021_avocats_2022, ',', 10) AS INTEGER) AS avocats_2006,
            CAST(SPLIT_PART(name_address_cour_appel_site_internet_numero_telephone_avocats_2002_avocats_2003_avocats_2004_avocats_2005_avocats_2006_avocats_2007_avocats_2008_avocats_2009_avocats_2010_avocats_2011_avocats_2012_avocats_2013_avocats_2014_avocats_2015_avocats_2016_avocats_2017_avocats_2018_avocats_2019_avocats_2020_avocats_2021_avocats_2022, ',', 11) AS INTEGER) AS avocats_2007,
            CAST(SPLIT_PART(name_address_cour_appel_site_internet_numero_telephone_avocats_2002_avocats_2003_avocats_2004_avocats_2005_avocats_2006_avocats_2007_avocats_2008_avocats_2009_avocats_2010_avocats_2011_avocats_2012_avocats_2013_avocats_2014_avocats_2015_avocats_2016_avocats_2017_avocats_2018_avocats_2019_avocats_2020_avocats_2021_avocats_2022, ',', 12) AS INTEGER) AS avocats_2008,
            CAST(SPLIT_PART(name_address_cour_appel_site_internet_numero_telephone_avocats_2002_avocats_2003_avocats_2004_avocats_2005_avocats_2006_avocats_2007_avocats_2008_avocats_2009_avocats_2010_avocats_2011_avocats_2012_avocats_2013_avocats_2014_avocats_2015_avocats_2016_avocats_2017_avocats_2018_avocats_2019_avocats_2020_avocats_2021_avocats_2022, ',', 13) AS INTEGER) AS avocats_2009,
            CAST(SPLIT_PART(name_address_cour_appel_site_internet_numero_telephone_avocats_2002_avocats_2003_avocats_2004_avocats_2005_avocats_2006_avocats_2007_avocats_2008_avocats_2009_avocats_2010_avocats_2011_avocats_2012_avocats_2013_avocats_2014_avocats_2015_avocats_2016_avocats_2017_avocats_2018_avocats_2019_avocats_2020_avocats_2021_avocats_2022, ',', 14) AS INTEGER) AS avocats_2010,
            CAST(SPLIT_PART(name_address_cour_appel_site_internet_numero_telephone_avocats_2002_avocats_2003_avocats_2004_avocats_2005_avocats_2006_avocats_2007_avocats_2008_avocats_2009_avocats_2010_avocats_2011_avocats_2012_avocats_2013_avocats_2014_avocats_2015_avocats_2016_avocats_2017_avocats_2018_avocats_2019_avocats_2020_avocats_2021_avocats_2022, ',', 15) AS INTEGER) AS avocats_2011,
            CAST(SPLIT_PART(name_address_cour_appel_site_internet_numero_telephone_avocats_2002_avocats_2003_avocats_2004_avocats_2005_avocats_2006_avocats_2007_avocats_2008_avocats_2009_avocats_2010_avocats_2011_avocats_2012_avocats_2013_avocats_2014_avocats_2015_avocats_2016_avocats_2017_avocats_2018_avocats_2019_avocats_2020_avocats_2021_avocats_2022, ',', 16) AS INTEGER) AS avocats_2012,
            CAST(SPLIT_PART(name_address_cour_appel_site_internet_numero_telephone_avocats_2002_avocats_2003_avocats_2004_avocats_2005_avocats_2006_avocats_2007_avocats_2008_avocats_2009_avocats_2010_avocats_2011_avocats_2012_avocats_2013_avocats_2014_avocats_2015_avocats_2016_avocats_2017_avocats_2018_avocats_2019_avocats_2020_avocats_2021_avocats_2022, ',', 17) AS INTEGER) AS avocats_2013,
            CAST(SPLIT_PART(name_address_cour_appel_site_internet_numero_telephone_avocats_2002_avocats_2003_avocats_2004_avocats_2005_avocats_2006_avocats_2007_avocats_2008_avocats_2009_avocats_2010_avocats_2011_avocats_2012_avocats_2013_avocats_2014_avocats_2015_avocats_2016_avocats_2017_avocats_2018_avocats_2019_avocats_2020_avocats_2021_avocats_2022, ',', 18) AS INTEGER) AS avocats_2014,
            CAST(SPLIT_PART(name_address_cour_appel_site_internet_numero_telephone_avocats_2002_avocats_2003_avocats_2004_avocats_2005_avocats_2006_avocats_2007_avocats_2008_avocats_2009_avocats_2010_avocats_2011_avocats_2012_avocats_2013_avocats_2014_avocats_2015_avocats_2016_avocats_2017_avocats_2018_avocats_2019_avocats_2020_avocats_2021_avocats_2022, ',', 19) AS INTEGER) AS avocats_2015,
            CAST(SPLIT_PART(name_address_cour_appel_site_internet_numero_telephone_avocats_2002_avocats_2003_avocats_2004_avocats_2005_avocats_2006_avocats_2007_avocats_2008_avocats_2009_avocats_2010_avocats_2011_avocats_2012_avocats_2013_avocats_2014_avocats_2015_avocats_2016_avocats_2017_avocats_2018_avocats_2019_avocats_2020_avocats_2021_avocats_2022, ',', 20) AS INTEGER) AS avocats_2016,
            CAST(SPLIT_PART(name_address_cour_appel_site_internet_numero_telephone_avocats_2002_avocats_2003_avocats_2004_avocats_2005_avocats_2006_avocats_2007_avocats_2008_avocats_2009_avocats_2010_avocats_2011_avocats_2012_avocats_2013_avocats_2014_avocats_2015_avocats_2016_avocats_2017_avocats_2018_avocats_2019_avocats_2020_avocats_2021_avocats_2022, ',', 21) AS INTEGER) AS avocats_2017,
            CAST(SPLIT_PART(name_address_cour_appel_site_internet_numero_telephone_avocats_2002_avocats_2003_avocats_2004_avocats_2005_avocats_2006_avocats_2007_avocats_2008_avocats_2009_avocats_2010_avocats_2011_avocats_2012_avocats_2013_avocats_2014_avocats_2015_avocats_2016_avocats_2017_avocats_2018_avocats_2019_avocats_2020_avocats_2021_avocats_2022, ',', 22) AS INTEGER) AS avocats_2018,
            CAST(SPLIT_PART(name_address_cour_appel_site_internet_numero_telephone_avocats_2002_avocats_2003_avocats_2004_avocats_2005_avocats_2006_avocats_2007_avocats_2008_avocats_2009_avocats_2010_avocats_2011_avocats_2012_avocats_2013_avocats_2014_avocats_2015_avocats_2016_avocats_2017_avocats_2018_avocats_2019_avocats_2020_avocats_2021_avocats_2022, ',', 23) AS INTEGER) AS avocats_2019,
            CAST(SPLIT_PART(name_address_cour_appel_site_internet_numero_telephone_avocats_2002_avocats_2003_avocats_2004_avocats_2005_avocats_2006_avocats_2007_avocats_2008_avocats_2009_avocats_2010_avocats_2011_avocats_2012_avocats_2013_avocats_2014_avocats_2015_avocats_2016_avocats_2017_avocats_2018_avocats_2019_avocats_2020_avocats_2021_avocats_2022, ',', 24) AS INTEGER) AS avocats_2020,
            CAST(SPLIT_PART(name_address_cour_appel_site_internet_numero_telephone_avocats_2002_avocats_2003_avocats_2004_avocats_2005_avocats_2006_avocats_2007_avocats_2008_avocats_2009_avocats_2010_avocats_2011_avocats_2012_avocats_2013_avocats_2014_avocats_2015_avocats_2016_avocats_2017_avocats_2018_avocats_2019_avocats_2020_avocats_2021_avocats_2022, ',', 25) AS INTEGER) AS avocats_2021,
            CAST(SPLIT_PART(name_address_cour_appel_site_internet_numero_telephone_avocats_2002_avocats_2003_avocats_2004_avocats_2005_avocats_2006_avocats_2007_avocats_2008_avocats_2009_avocats_2010_avocats_2011_avocats_2012_avocats_2013_avocats_2014_avocats_2015_avocats_2016_avocats_2017_avocats_2018_avocats_2019_avocats_2020_avocats_2021_avocats_2022, ',', 26) AS INTEGER) AS avocats_2022
    FROM avocats_data.avocats_data
)
SELECT 
    json_group_array(
        json_object(
               'name', name,
                'avocats_2002', avocats_2002,
                'avocats_2003', avocats_2003,
                'avocats_2004', avocats_2004,
                'avocats_2005', avocats_2005,
                'avocats_2006', avocats_2006,
                'avocats_2007', avocats_2007,
                'avocats_2008', avocats_2008,
                'avocats_2009', avocats_2009,
                'avocats_2010', avocats_2010,
                'avocats_2011', avocats_2011,
                'avocats_2012', avocats_2012,
                'avocats_2013', avocats_2013,
                'avocats_2014', avocats_2014,
                'avocats_2015', avocats_2015,
                'avocats_2016', avocats_2016,
                'avocats_2017', avocats_2017,
                'avocats_2018', avocats_2018,
                'avocats_2019', avocats_2019,
                'avocats_2020', avocats_2020,
                'avocats_2021', avocats_2021,
                'avocats_2022', avocats_2022
        )
    ) AS avocats_analysis_json
INTO 'avocats_analysis.json'
FROM split_data
ORDER BY name;