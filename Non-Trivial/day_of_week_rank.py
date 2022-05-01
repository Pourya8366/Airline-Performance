#! /usr/bin/python3
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRWordCount(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapperDelay, reducer=self.reducerDelay
            ),
            MRStep(
                reducer=self.reducerSort
            )
        ]
    def mapperDelay(self, _, line):

        flight = line.split(",")

        #flight_stats:
        #0: arrDelay
        #1: if cancelled: 0: not cancelled, 1: cancelled
        delay = 0

        #Check if flight was not cancelled and not diverted to have arrdelay value
        if flight[49] == "0.00" and flight[51] == "0.00" and flight[44] != "":
            # 1. add arrDelay
            #tip: when no delay, valude would be ""
            delay = float(flight[44])

        yield [flight[0].strip('"'), flight[2].strip('"'), flight[4].strip('"')], delay

    def reducerDelay(self, key, delays):
        delays = list(delays)
        
        avg = sum(delays) / len(delays)

        yield None, (key, avg)

    def reducerSort(self, key, avgs):

        delays = sorted(avgs, key=lambda tup: tup[1], reverse=True)

        yield None, delays
if __name__ == '__main__':
    MRWordCount.run()