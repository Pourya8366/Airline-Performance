select Origin as airport 
from flights 
union 
select Dest 
from flights

select Origin as src, Dest as dst, Reporting_Airline as relationship, sum(cast(DepDelay as int)) as Departure_Delay, sum(cast(ArrDelay as int)) as Arrival_Delay, sum(cast(ActualElapsedTime as int) - cast(CRSElapsedTime as int)) as Wasted_Time, sum(cast(Cancelled as int)) as Cancelled, sum(cast(Diverted as int)) as Diverted, count(*) as Flights_Count 
from flights 
group by Origin, Dest, Reporting_Airline


select Origin as src, Dest as dst, Reporting_Airline as airline, Year, Quarter, Month, DayofMonth, DayOfWeek, Origin, OriginStateName, Dest, DestStateName, DepDelay, ArrDelay, CRSDepTime, DepTime, Cancelled, CRSElapsedTime, ActualElapsedTime, Distance from flights 