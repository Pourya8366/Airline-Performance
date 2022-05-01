#! /usr/bin/python3
from mrjob.job import MRJob

class MRWordCount(MRJob):
    
    def mapper(self, _, line):

        #flight[49]: Cancelled -> 0, 1
        #flight[14]: origin airport

        flight = line.split(",")

        #Check if flight was not cancelled
        if flight[49] == "0.00":
            yield flight[14].strip('"'), 1

    def combiner(self, origin_airport, flights):
         yield origin_airport, sum(flights)
    
    def reducer(self, origin_airport, flights):
         yield origin_airport, sum(flights)

        
if __name__ == '__main__':
    MRWordCount.run()