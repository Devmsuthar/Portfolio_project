-- selecting the database to use 
use project;

-- creating a new table and using varchar datatype for int to import the data fast and without error as the table has null values in it 
create table airbnb(
id int,
NAME varchar(255) ,
host_id bigint ,
host_identity_verified varchar(100) ,
host_name varchar(255) ,
neighbourhood_group varchar(255) ,
neighbourhood varchar(255) ,
lat	varchar(255) ,
longg varchar(255) ,	
country varchar(100) ,	
country_code varchar(100) ,	
instant_bookable varchar(100) ,	
cancellation_policy varchar(100) ,	
room_type varchar(255) ,	
Construction_year varchar(255) ,	
price varchar(255) ,	
service_fee varchar(255) ,	
minimum_nights varchar(255) ,	
number_of_reviews varchar(255) ,	
last_review varchar(255) ,
reviews_per_month varchar(255) ,	
review_rate_number varchar(255) ,	
calculated_host_listings_count varchar(255) ,	
availability_365 varchar(255) ,	
house_rules	longtext ,
license varchar(255));
 ------------------------------------------------------------------------------------------------------------
 
-- now to import the data to the table using load file function
load data infile 'C:/Users/admin/OneDrive/data_analysis/Airbnb_Open_Data_mysql.csv'
into table project.airbnb
fields terminated by ',' OPTIONALLY ENCLOSED BY '"'
lines terminated by '\n'
ignore 1 rows
-- this is to import data even if the values are null 
(id, NAME, host_id, host_identity_verified,	host_name, neighbourhood_group,	neighbourhood, lat,	longg, country,	country_code, instant_bookable,	cancellation_policy, room_type, Construction_year, price, service_fee, minimum_nights,	number_of_reviews,	last_review, reviews_per_month,	review_rate_number,	calculated_host_listings_count,	availability_365, house_rules, license)
set id = nullif(id, ''),
NAME = nullif(NAME, ''),
host_id = nullif(host_id, ''),
host_identity_verified = nullif(host_identity_verified, ''),
host_name = nullif(host_name, ''),
neighbourhood_group = nullif(neighbourhood_group, ''),
neighbourhood = nullif(neighbourhood, ''),
lat = nullif(lat, ''),
longg = nullif(longg, ''),
country = nullif(country, ''),
country_code = nullif(country_code, ''),
instant_bookable = nullif(instant_bookable, ''),
cancellation_policy = nullif(cancellation_policy, ''),
room_type = nullif(room_type, ''),
Construction_year = nullif(Construction_year, ''),
price = nullif(price, ''),
service_fee = nullif(service_fee, ''),
minimum_nights = nullif(minimum_nights, ''),
number_of_reviews = nullif(number_of_reviews, ''),
last_review = nullif(last_review, ''),
reviews_per_month = nullif(reviews_per_month, ''),
review_rate_number = nullif(review_rate_number, ''),
calculated_host_listings_count = nullif(calculated_host_listings_count, ''),
availability_365 = nullif(availability_365, ''),
house_rules = nullif(house_rules, ''),
license = nullif(license, '');
-----------------------------------------------------------------------------------------------------------

-- change the datatype of the columns
alter table airbnb modify column lat double null;
alter table airbnb modify column longg double null;
alter table airbnb modify column construction_year int null;
alter table airbnb modify column price int null;
alter table airbnb modify column service_fee int null;
alter table airbnb modify column minimum_nights int null;
alter table airbnb modify column number_of_reviews int null;
alter table airbnb modify column reviews_per_month int null;
alter table airbnb modify column review_rate_number int null;
alter table airbnb modify column calculated_host_listings_count int null;
alter table airbnb modify column availability_365 int null;
alter table airbnb modify column last_review date null;

-- to view the schema of the table 
describe project.airbnb; # all the datatypes have been updated as the correct datatypes

-- now to view the data
select * from airbnb;

-------------------------------------------------------------------------------------------------------------

-- first is to check whether there is a duplicate 
select id, count(*) as con from airbnb 
group by id
having count(*) > 1; -- there is 541 duplicate id number 

-- this is used to create temp_table that has all unique values 
create temporary table temp_table as
select distinct * from airbnb; -- this will select all the distinct values from all the columns

-- then deleting all the values from the existing table 
delete from airbnb;
 
-- inserting all the unique values from the temp table  
insert into airbnb 
select * from temp_table; -- 102058 records affected 

-- lets delete the temp table 
drop table temp_table;
-------------------------------------------------------------------------------------------------------------
-- lets fill the missing data for some columns to make the dataset clean 
update airbnb set NAME = 'room'
where name is null; -- 249 rows updated. as the name depends on the type of structure the room has so there's no way to fill the accurate data  

update airbnb set host_identity_verified = 'under review'
where host_identity_verified = 'unconfirmed or verified'; -- 289 rows updated 
-------------------------------------------------------------------------------------------------------------
-- to find the host name with the neighbourhood group 
select *  from airbnb
where host_name in (select distinct(host_name) from airbnb); # there are thousand of records so it difficult to fill the missing data 

-- lets update the missing host name with under review
update airbnb set host_name = 'under review'
where host_name is null; # 404 rows updated 

--------------------------------------------------------------------------------------------------------------
-- to find the distinct neighbourhood_group column 
select  distinct neighbourhood_group from airbnb; # we have Brooklyn, brookln, Manhattan , manhatten. had 8 rows result

-- lets update the column 
update airbnb set neighbourhood_group = 'Brooklyn' 
where neighbourhood_group = 'brookln';
update airbnb set neighbourhood_group = 'Manhattan' 
where neighbourhood_group = 'manhatan';

-- lets check if there are records changed 
select neighbourhood_group from airbnb
where neighbourhood_group = 'manhaten'; -- error output so the data have been updated

select distinct neighbourhood, neighbourhood_group from airbnb 
where neighbourhood_group is null; # 15 rows missing data 

select distinct neighbourhood_group, neighbourhood from airbnb # found the records from the previous query that had missing values and based on that updated the missing values 
where neighbourhood in('Washington Heights', -- man
'Clinton Hill', -- br
'East Village', -- man
'Upper East Side', -- man
'Woodside', -- queens
'Williamsburg', -- brokk
'Bushwick', -- bro
'Prospect Heights', -- br
'Chelsea', -- br
'East Harlem', -- man
'Eastchester', -- bronx
'Harlem', -- man
'Chinatown', -- man
'Queens Village',  -- queens 
'Bedford-Stuyvesant', -- brooklyn
'Upper West Side'); -- manhatten

-- used case statement to update multiple rows with different values that had null 

update airbnb set 
neighbourhood_group = 
 case 
 when neighbourhood = 'Clinton Hill' then 'brooklyn'
 when neighbourhood = 'East Village' then 'Manhatten'
 when neighbourhood = 'Upper East Side' then 'Manhatten'
 when neighbourhood = 'East Village' then 'Manhatten'
 when neighbourhood = 'Woodside' then 'Queens'
 when neighbourhood = 'Williamsburg' then 'Brooklyn'
 when neighbourhood = 'Bushwick' then 'Brooklyn'
 when neighbourhood = 'Prospect Heights' then 'Brooklyn'
 when neighbourhood = 'Chelsea' then 'Brooklyn'
 when neighbourhood = 'East Harlem' then 'Manhatten'
 when neighbourhood = 'Eastchester' then 'Bronx'
 when neighbourhood = 'Harlem' then 'Manhatten'
 when neighbourhood = 'Chinatown' then 'Manhatten'
 when neighbourhood = 'Queens Village' then 'Queens'
 when neighbourhood = 'Bedford-Stuyvesant' then 'Brooklyn'
 when neighbourhood = 'Upper West Side' then 'Manhatten'
 end
 where neighbourhood in ('Clinton Hill', 'East Village','Upper East Side','East Village',
 'Woodside','Williamsburg','Bushwick','Prospect Heights','Chelsea','East Harlem',
 'Eastchester','Harlem','Chinatown','Queens Village','Bedford-Stuyvesant','Upper West Side') 
 and neighbourhood_group is null; -- the null values in the neighbourhood group had been filled with relevent data from the neighbour column

---------------------------------------------------------------------------------------------------------------
-- to check and update neighbourhood column missing data and update it 
select  neighbourhood, neighbourhood_group from airbnb 
where neighbourhood is null; # 2 distinct rows missing data. since there are multiple neighbour for each neighourhood group, we can update it as under review

update airbnb set neighbourhood = 'under review', neighbourhood = 'under review'
where neighbourhood_group in ('Brooklyn', 'Manhattan') and neighbourhood is null; -- all records have been updated 

---------------------------------------------------------------------------------------------------------------

-- to fill the lat and longg values using neighbourhood column data

select  distinct neighbourhood , lat, longg from airbnb  
where lat is null and longg is null;

select distinct neighbourhood , lat, longg from airbnb  
where neighbourhood in ('Crown Heights','Elmhurst','Greenpoint','East Village','West Village','Flatiron District','Upper West Side');

 ## Crown Heights 40.68 -73.95 Elmhurst 40.74 -73.88 Greenpoint 40.74 -73.95 East Village	40.73	-73.98 West Village	40.73	-74 Flatiron District	40.74	-73.99 Upper West Side	40.79	-73.96

update airbnb set lat  = 40.68
where neighbourhood = 'Crown Heights' and lat is null;
update airbnb set longg  = -73.95
where neighbourhood = 'Crown Heights' and longg is null;
update airbnb set lat  = 40.74, longg = -73.88
where neighbourhood = 'Elmhurst' and lat is null and longg is null;
update airbnb set lat  = 40.74, longg = -73.95
where neighbourhood = 'Greenpoint' and lat is null and longg is null;
update airbnb set lat  = 40.73, longg = -73.98
where neighbourhood = 'East Village' and lat is null and longg is null;
update airbnb set lat  = 40.73, longg = -74
where neighbourhood = 'West Village' and lat is null and longg is null;
update airbnb set lat  = 40.74, longg = -73.99
where neighbourhood = 'Flatiron District' and lat is null and longg is null;
update airbnb set lat  = 40.79, longg = -73.96
where neighbourhood = 'Upper West Side' and lat is null and longg is null;

-------------------------------------------------------------------------------------------------------------

-- lets update the missing values in country and country code column 
select  country , country_code from airbnb; # so both column have missing data relating to united states

-- so lets update both the entire column as the data is showing us
update airbnb set country = 'united states';
update airbnb set country_code = 'us'; -- all the missiing data have been updated 

-------------------------------------------------------------------------------------------------------------

-- lets update instant bookable and cancellation policy column since we cannot derive the relationship between them lets update it to under review for null values

update airbnb set instant_bookable = 'under review' where instant_bookable is null; -- 105 rows values updated 
update airbnb set cancellation_policy = 'under review' where cancellation_policy is null; -- 76 rows values updated
# all the missing values values have been updated 

----------------------------------------------------------------------------------------------------------------------

-- lets update construction year since there is no correlation to fill the missing data lets update it as 1 since its a date datatype

update airbnb set construction_year = 1
where construction_year is null; # 214 rows updated 
# all the missing date have been filled 

----------------------------------------------------------------------------------------------------------------------

-- lets fill the missing data from price and service fee column 

select price, service_fee from airbnb
where service_fee is null;-- 273 null rows

select price, service_fee from airbnb
where price is null; -- 247 null rows 

update airbnb set price = service_fee * 5 # the correlation between the price is five times the rate of price, for example 100 is the service fee so the price is 100 * 5 and the result is the actual price.
where price is null; -- 213 rows updated

update airbnb set service_fee = price * 0.2
where service_fee is null;-- 239 rows updated

select price, service_fee from airbnb
where price is null and service_fee is null; -- 34 rows null

update airbnb set service_fee = 0 , price = 0 
where service_fee is null and price is null; -- all the rows which was missing values are updated with 0 

---------------------------------------------------------------------------------------------------------------------

-- lets update the minimum nights column since there no relationship from other columns to fing the missing value lets update it with 1

update airbnb set minimum_nights = 1
where minimum_nights is null; -- 400 rows updated 

---------------------------------------------------------------------------------------------------------------------
-- lets update the number of reviews, last review , reviews per month, review rate number and calculated host columns since there no relationship from other columns to fing the missing value lets update it with 0

update airbnb set number_of_reviews = 1 where number_of_reviews is null; -- 183 rows updated

alter table airbnb modify column last_review varchar(100) null; -- since it was date datatype to fill the missing values, converted it to varchar 
update airbnb set last_review = 'under review'  where last_review is null; -- 15832 rows updated 

select last_review from airbnb
where last_review > '2022';-- since there values where last review year is after 2022 so that data must be updated with under review 

update airbnb set last_review = 'under review'  where last_review > '2022'; -- 26206 rows updated 


update airbnb set reviews_per_month = 1 where reviews_per_month is null; -- 15818  rows updated 

update airbnb set review_rate_number = 1 where review_rate_number is null; -- 319  rows updated 

update airbnb set calculated_host_listings_count = 1 where calculated_host_listings_count is null; -- 319  rows updated 


-----------------------------------------------------------------------------------------------------------------------------

-- lets update the avaliability 365 column as there are values that exceed the 365days so lets change that values to 365 

update airbnb set availability_365 = 365 
where availability_365 > 365; -- 2754 rows updated 

-- lets update the house rules column since there are null values which can mean either there no rules or not mentioned
update airbnb set house_rules = 'no rules or not mentioned'
where house_rules is null; -- 51842 rows updated 


-- since whole license column is null lets update it with under review 
update airbnb set license = 'under review'; -- 102058 rows updated 

------------------------------------------------ XXXXXX --------------------------------------------------------------
-- all the data have been cleaned and missing values have been filled ---






















