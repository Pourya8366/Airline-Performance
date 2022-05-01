-- select unique airports as vertices
select Origin as airport 
from flights 
union 
select Dest 
from flights

-- select fields from flights as edges
select Origin as src, Dest as dst, Reporting_Airline as airline, Year, Quarter, Month, DayofMonth, DayOfWeek, Origin, OriginStateName, OriginCityName, Dest, DestStateName, DestCityName, cast(DepDelay as int) as Departure_delay, cast(ArrDelay as int) as Arrival_Delay, DepTimeBlk, cast(Cancelled as int) as Cancelled, cast(CRSElapsedTime as int) as CRSElapsed_Time, cast(ActualElapsedTime as int) as Actual_Elapsed_Time, cast(Distance as int) as Distance from flights

---- Time ---- 

-- year-based arrival delay
select Year, avg(ArrDelay) as Arrival_Delay, sum(Cancelled) as Total_Cancelled from stats group by Year order by Arrival_Delay desc, Total_Cancelled desc

-- month-based arrival delay
select Year, Month, avg(ArrDelay) as Arrival_Delay, sum(Cancelled) as Total_Cancelled from stats group by Year, Month order by Arrival_Delay desc, Total_Cancelled desc

-- day-based arrival delay
select Year, Month, DayofMonth as Day, avg(ArrDelay) as Arrival_Delay, sum(Cancelled) as Total_Cancelled from stats group by Year, Month, Day order by Arrival_Delay desc, Total_Cancelled desc

-- dayOfWeek-based arrival delay
select Year, Month, DayOfWeek, avg(ArrDelay) as Arrival_Delay, sum(Cancelled) as Total_Cancelled from stats group by Year, Month, DayOfWeek order by Arrival_Delay desc, Total_Cancelled desc

-- hourly delay
select DepTimeBlk, avg(ArrDelay) as Arrival_Delay from stats group by DepTimeBlk order by Arrival_Delay desc

---- Airline ----

-- ailrine ranking worst to best
select airline, avg(ArrDelay) as Arrival_Delay, sum(Cancelled) as Total_Cancelled from stats group by airline order by Arrival_Delay desc, Total_Cancelled desc

-- airline yearly based
airline_yearly = select Year, airline, avg(ArrDelay) as Arrival_Delay, sum(Cancelled) as Total_Cancelled from stats group by Year, airline 

-- worst airline of each year
select Year, airline, max(Arrival_Delay) as Arrival_Delay from airline_yearly group by Year

-- best airline of each year
select Year, airline, min(Arrival_Delay) as Arrival_Delay from airline_yearly group by Year

-- airline performance
select Year, Month, airline, avg(ArrDelay) as Arrival_Delay, sum(Cancelled) as Total_Cancelled from stats group by Year, Month order by Arrival_Delay desc, Total_Cancelled desc

-- longest distance
select src, dst, max(Distance) as Distance from stats group by src, dst order by Distance

select Distance, avg(ArrDelay) as Arrival_Delay from stats group by Distance order by Arrival_Delay desc

---- airports ----
select src, dst, avg(Arrival_Delay) as Arrival_Delay from stats group by src, dst order by Arrival_Delay desc

-- number of flights
select src, dst, count(*) as NumberOfFlights from stats group by src, dst order by NumberOfFlights desc

select Year, Month, DayOfWeek , avg(ArrDelay) as Arrival_Delay from stats group by Year, Month, DayOfWeek order by Arrival_Delay desc