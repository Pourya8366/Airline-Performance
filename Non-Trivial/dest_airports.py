#! /usr/bin/python3
from mrjob.job import MRJob

class MRWordCount(MRJob):
    
    def mapper(self, _, line):

        #flight[49]: Cancelled -> 0, 1
        #flight[24]: destination airport

        flight = line.split(",")

        #Check if flight was not cancelled
        if flight[49] == "0.00":
            yield flight[24].strip('"'), 1

    def combiner(self, dest_airport, flights):
         yield dest_airport, sum(flights)
    
    def reducer(self, dest_airport, flights):
         yield dest_airport, sum(flights)         

        
if __name__ == '__main__':
    MRWordCount.run()