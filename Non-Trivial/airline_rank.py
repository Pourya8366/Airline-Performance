#! /usr/bin/python3
from mrjob.job import MRJob

class MRWordCount(MRJob):
    
    def mapper(self, _, line):

        flight = line.split(",")

        #flight_stats:
        #0: depDelay
        #1: arrDelay
        #2: exceeded time in plane: ActualElapsedTime - CRSElapsedTime
        #3: if cancelled: 0: not cancelled, 1: cancelled
        #4: if diverted: 0: not diverted, 1: diverted
        #5: 1 flight
        flight_stats = [0.0, 0.0, 0.0, 1, 1, 1]

        
        #flight[33]: DepDelay
        #flight[44]: ArrDelay
        #flight[49]: Cancelled: 0.00 or 1.00
        #flight[51]: Diverted: 0.00 or 1.00
        #flight[53]: ActualElapsedTime
        #flight[52]: CRSElapsedTime

        #Check if flight was not cancelled
        if flight[49] == "0.00" and flight[51] == "0.00":
            # 1. add depDelay
            #tip: when no delay, valude would be ""
            flight_stats[0] = float(flight[33]) if flight[33] != "" else 0.0

            # 2. add arrDelay
            flight_stats[1] = float(flight[44]) if flight[44] != "" else 0.0

            # 3. exceeded time in plane
            flight_stats[2] = float(flight[53]) - float(flight[52])
        
            # 4. not cancelled
            flight_stats[3] = 0

            # 5. not diverted
            flight_stats[4] = 0
            
        elif flight[51] == "0.00":
            flight_stats[4] = 0

        #flight[14]: origin airport
        #flight[24]: destination airport
        #flight[6]: airline
        yield [flight[14].strip('"'), flight[24].strip('"'), flight[6].strip('"')], flight_stats

    def combiner(self, airport_flight, flights_stats):
        flights_stats = list(flights_stats)

        # for each key: origin_airport, dest_airport, airline
        #0: total depDelay
        #1: total arrDelay
        #2: total exceeded time in plane: ActualElapsedTime - CRSElapsedTime
        #3: no. cancelled
        #4: no. diverted
        #5: no. flights
        stats = [0.0, 0.0, 0.0, 0, 0, 0]
        
        for flight_stat in flights_stats:
            stats = [stats[i]+flight_stat[i] for i in range(6)]

        yield airport_flight, stats

    def reducer(self, airport_flight, flights_stats):
        flights_stats = list(flights_stats)

        # for each key: origin_airport, dest_airport, airline
        #0: total depDelay
        #1: total arrDelay
        #2: total exceeded time in plane: ActualElapsedTime - CRSElapsedTime
        #3: no. cancelled
        #4: no. diverted
        #5: no. flights
        stats = [0.0, 0.0, 0.0, 0, 0, 0]
        
        for flight_stat in flights_stats:
            stats = [stats[i]+flight_stat[i] for i in range(6)]

        yield tuple(airport_flight), tuple(stats)

        
if __name__ == '__main__':
    MRWordCount.run()