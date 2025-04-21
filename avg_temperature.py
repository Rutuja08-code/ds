%%writefile avg_temperature.py
from mrjob.job import MRJob
import csv

class AvgTemperature(MRJob):

    def mapper(self, _, line):
        if not line.startswith("month"):  # skip header
            fields = list(csv.reader([line]))[0]
            temp = float(fields[6])  # index of temperature
            yield "AverageTemperature", temp

    def reducer(self, key, temps):
        temps = list(temps)
        yield key, sum(temps) / len(temps)

if __name__ == '__main__':
    AvgTemperature.run()
