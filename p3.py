from mrjob.job import MRJob
from mrjob.step import MRStep
#from mrjob.protocol import JSONValueProtocol
from mr3px.csvprotocol import CsvProtocol
from mrjob.conf import combine_values
import re
import itertools

class MRWordFrequencyCount(MRJob):
       # INPUT_PROTOCOL = JSONValueProtocol
        INPUT_PROTOCOL = CsvProtocol
        OUTPUT_PROCOTOL = CsvProtocol

        def mapper(self, _, line):
                yield[line[1],(line[2])]

	def reducer2(self, movie, values):
		aux = []
		for x in values:
			aux.append(x)
		yield [movie,combine_values(aux)]


	def reducer3(self, movie, values):
		values = list(values)

		for x in values:
			yield [movie,(x,len(x)) ]	


	def reducer4(self, movie, values):
		suma = 0
		prom = 0
		for x in values:
			rank_list = x[0]
			for r in rank_list:
				try:
					suma = suma + float(r)
				except:
					print "errr"
			prom = suma / x[1]
		yield None ,(movie,prom)	


	def reducer5(self, non_value, values):
		prom = 0
		comb_list = list(itertools.combinations(values,2))
		for x in comb_list:
			prom = (x[0][1]+x[1][1])/2
			yield None ,(prom,(x[0][0],x[1][0]) )
		
	def reducer6(self, none_value,values):
		yield min(values)
#		yield min(values)
	
        def steps(self):
                return[
                        MRStep(mapper = self.mapper, reducer = self.reducer2),
			MRStep(reducer = self.reducer3),
			MRStep(reducer = self.reducer4),
			MRStep(reducer = self.reducer5),
			MRStep(reducer = self.reducer6)
                ]





if __name__ == '__main__':
    MRWordFrequencyCount.run()

