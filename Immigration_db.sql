-- drop tables with CASCADE method

DROP TABLE IF EXISTS countries CASCADE;
DROP TABLE IF EXISTS immigration CASCADE;
DROP TABLE IF EXISTS macroecodata CASCADE;


-- 1) Create tables


		--  Drop table if exists
		DROP TABLE IF EXISTS countries;
		
		-- Create new table	

		CREATE TABLE countries (
								iso3Code char(3) not null,
								iso2Code char(2) not null,
								country VARCHAR(60) not null primary key,
								region  VARCHAR(150),
								capitalCity  VARCHAR(50),
								longitude FLOAT,
								latitude FLOAT

								) ;
		SELECT * FROM countries ;

		-- IMPORT DATA

		COPY countries (iso3Code,iso2Code,country,region,capitalCity,longitude,latitude)
		FROM 'F:/github/project3_group4/Output/countries_list_UN_referential.csv'
		DELIMITER ','
		CSV HEADER;

		SELECT * FROM countries ;


		--  Drop table if exists
				
		DROP TABLE IF EXISTS immigration ;
		
		-- Create new table	

		CREATE TABLE immigration (
								index serial not null primary key,
								country VARCHAR(60),
								year int ,
								month_str VARCHAR(3),
								month_int VARCHAR(2),
								quarter VARCHAR(2),
								immigration_flow int 

								) ;
								
				SELECT * FROM immigration ;

		-- IMPORT DATA

		COPY immigration (index,country,year,month_str,month_int,quarter,immigration_flow)
		FROM 'F:/github/project3_group4/Output/immigrants_by_country_monthly.csv'
		DELIMITER ','
		CSV HEADER;


		SELECT * FROM immigration ;	


		--  Drop table if exists
				
		DROP TABLE IF EXISTS macrodata ;
		
		-- Create new table	

		CREATE TABLE macrodata (
								index serial NOT NULL PRIMARY KEY,
								iso3Code char(3),
								iso2Code char(2),
								country VARCHAR(60),
								year INT,
								indicator VARCHAR(150),
								value FLOAT
								) ;
								
				SELECT * FROM macrodata ;


		-- IMPORT DATA

		COPY macrodata (index ,iso3Code,iso2Code,country,year,indicator,value)
		FROM 'F:/github/project3_group4/Output/macro_economic_data.csv'
		DELIMITER ','
		CSV HEADER;

		SELECT * FROM macrodata ;