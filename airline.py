#! /usr/bin/python3
from mrjob.job import MRJob

class MRWordCount(MRJob):
    
    def mapper(self, _, line):
        
        row = line.split(",")
        delay = 0
        #row[33]: DepDelay
        #row[44]: ArrDelay

        #Check if flight was not cancelled
        if row[33] and row[44]:
            delay = int(row[33]) + int(row[44])

        #row[14]: origin airport
        #row[24]: destination airport
        #row[6]: airline
        yield (row[14], row[24]), (row[6], delay)

    
    def reducer(self, airports, flights):
        #flights:
        #[0]: airline
        #[1]: total delay
        flights = list(flights)
        
        #flights_dict:
        #{airline: [flights_count, total_delay]}
        flights_dict = {}

        #total flights for each airport pair (origin, dest)
        flights_count = 0
        
        for flight in flights:
            flights_dict.setdefault(flight[0], [0, 0])

            flights_dict[flight[0]] = [flights_dict[flight[0]][0]+1, flights_dict[flight[0]][1]+ flight[1]]

            flights_count += 1
        
        yield (airports, flights_count), flights_dict

        
if __name__ == '__main__':
    MRWordCount.run()