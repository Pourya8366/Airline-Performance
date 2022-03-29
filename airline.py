#! /usr/bin/python3
from mrjob.job import MRJob

class MRWordCount(MRJob):
    
    def mapper(self, _, line):

        flight = line.split(",")

        #flight_detail:
        #0: airline
        #1: depDelay
        #2: arrDelay
        #3: exceeded time in plane: ActualElapsedTime - CRSElapsedTime
        #4: if cancelled: 0: not cancelled, 1: cancelled
        #5: if diverted: 0: not diverted, 1: diverted

        # 1. add airline
        flight_detail = [flight[6].strip('"'), 0.0, 0.0, 0.0, 1, 1]

        #flight[6]: airline
        #flight[33]: DepDelay
        #flight[44]: ArrDelay
        #flight[49]: Cancelled: 0.00 or 1.00
        #flight[51]: Diverted: 0.00 or 1.00
        #flight[53]: ActualElapsedTime
        #flight[52]: CRSElapsedTime

        #Check if flight was not cancelled
        if flight[49] == "0.00" and flight[51] == "0.00":
            # 2. add depDelay
            flight_detail[1] = float(flight[33])

            # 3. add arrDelay
            flight_detail[2] = float(flight[44])

            # 4. exceeded time in plane
            flight_detail[3] = float(flight[53]) - float(flight[52])
        
            #5. not cancelled
            flight_detail[4] = 0
            flight_detail[5] = 0
            
        elif flight[51] == "1.00":
            flight_detail[5] = 1

        #flight[14]: origin airport
        #flight[24]: destination airport
        yield [flight[14].strip('"'), flight[24].strip('"')], flight_detail

    
    def reducer(self, airports, flights):
        flights = list(flights)
        
        #total flights for each airport pair (origin, dest)
        flights_count = 0

        #key: airline
        #values:
        #0: totalDepDelay
        #1: totalArrDelay
        #2: total exeed time in plane
        #3: no. flights
        #4: no. cancelled
        flights_dict = {}
        
        # flight:
        #0: airline
        #1: depDelay
        #2: arrDelay
        #3: exceeded time in plane: ActualElapsedTime - CRSElapsedTime
        #4: if cancelled: 0: not cancelled, 1: cancelled
        for flight in flights:
            airline = flight[0]
            flights_dict.setdefault(airline, [0.0, 0.0, 0.0, 0, 0])
            airline_dict = flights_dict[airline]

            airline_dict = [airline_dict[0] + flight[1], airline_dict[1] + flight[2], airline_dict[2] + flight[3], airline_dict[3] + 1, airline_dict[4]+ flight[4]]

            flights_count += 1
        
        yield [airports, flights_count], flights_dict

        
if __name__ == '__main__':
    MRWordCount.run()