#! /usr/bin/python3
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRWordCount(MRJob):

    def mapper(self, _, line):

        flight = line.split(",")

        #flight[56]: Distance
        #flight[14]: origin airport
        #flight[24]: destination airport

        yield flight[14].strip('"'), flight[24].strip('"')

    def reducer(self, origin, adjacents):
        adjacents = set(adjacents)
        result = {}

        for adjacent in adjacents:
            result[adjacent] = 1

        yield origin, result

if __name__ == '__main__':
    MRWordCount.run()