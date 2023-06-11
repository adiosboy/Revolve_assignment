select * from planes;
select * from flights ;
select * from airlines;
select * from airports;

--QUESTION1->Which manufacturer's planes had most no of flights? And how many flights?

-- to check the tailnum is primary key or not

select tailnum from planes where tailnum is null; 

select tailnum,count(*) from planes 
group by tailnum
having count(*)>1;

--so tailnum is primary key in planes table


select count(*) from planes;
select count(*) from flights;

--so flights is bigger table has more rows than planes,joining the bigger table to smaller one for better optimization

select p.manufacturer, sum(f.flight) as no_of_flights from flights f inner join planes p  
on p.tailnum=f.tailnum
group by p.manufacturer
order by no_of_flights desc
limit 1 ;


--QUESTION2->Which manufacturer's planes had most no of flying hours? And how many hours?

--updated the column because it had "NA" values hence aggregation made paossible
UPDATE flights
SET hour  = COALESCE(NULLIF(hour, 'NA'), '0');

UPDATE flights
SET minute  = COALESCE(NULLIF(minute , 'NA'), '0');

commit;

select (cast(hour as decimal) + round((cast (minute as decimal)/60),2) ) as  total_hours from flights
order by total_hours desc;

with cte as (
select p.manufacturer as manufacturer, cast(f.hour as decimal) + round((cast (f.minute as decimal)/60),2) as total_hours
from flights f inner join planes p  
on p.tailnum=f.tailnum
order by total_hours desc)
select manufacturer,sum(total_hours) as hours from cte
group by manufacturer
order by hours desc
limit 1;


--Question3-> Which plane flew the most number of hours? And how many hours?

--to check any duplicate values are present in tailnum coulmn of flights table
select tailnum,count(*)  from flights
group by tailnum having count(*)>1;


select f.tailnum, sum(cast(f.hour as decimal) + round((cast (f.minute as decimal)/60),2)) as total_hours 
from flights f inner join planes p  
on p.tailnum=f.tailnum
group by f.tailnum
order by total_hours desc
limit 1;

commit;

--Question4->Which destination had most delay in flights?;

--first checked for "NA" values in the columns 
select dep_delay from flights where dep_delay = 'NA';
select arr_delay from flights where arr_delay = 'NA';

--There were "NA" values present so updated the table and removed the "NA" values and given 0 min time to that
UPDATE flights
SET dep_delay = COALESCE(NULLIF(dep_delay, 'NA'), '0');
rollback;

UPDATE flights
SET arr_delay = COALESCE(NULLIF(arr_delay, 'NA'), '0');
rollback;

select sum(cast (dep_delay as decimal )+ cast (arr_delay as decimal)) as delay_time , dest from flights
group by dest
order by delay_time desc 
limit 1;

--Question5->Which manufactures planes had covered most distance? And how much distance?


select p.manufacturer , sum(f.distance) as total_distance from planes p inner join flights f 
on p.tailnum =f.tailnum 
group by p.manufacturer 
order by total_distance desc limit 1;


--Question6-> Which airport had most flights on weekends?

 -- we needed the day column to find the flights on weekend so added the new_day column for day 
 -- using day,month and year columns in the table updated the new_day column 
alter table flights add column new_day text ;

update flights set new_day= to_char(make_date(year::integer, month::integer, day::integer), 'Day');

select "AIRPORT", count(tailnum) as no_of_dep_flights from airports a inner join flights f on a."IATA_CODE" = f.origin
where trim(new_day) = 'Sunday' or trim(new_day)= 'Saturday'
group by "AIRPORT"
order by no_of_dep_flights desc
limit 1;




#