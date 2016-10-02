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
		yield[line[0],(line[1])]

			

	def reducer2(self, movie, iterable):
                aux = []
                for x in iterable:
                        aux.append(x)
                        
                yield [movie,combine_values(aux)]
		#yield None ,(combine_values(aux))

	def test_re(self, key, itera):
		aux = []

		for x in itera:
			aux.append(x)
		yield combine_values(aux),1

	def count_user_movies(self, user, movies):
		
		ne = list(movies)
		for movie_list in ne:
			for movie in movie_list:
			#	yield None ,((user, len(movie_list)), movie)
				yield movie, (user, len(movie_list))

	def join_movies(self, user , movie):
		vals = []
		for x in movie:		
			if len(x) > 1:
				for val in x:
					vals.append(val)
		if len(vals) > 1:
			for x in list(itertools.combinations(vals, 2)):
				yield x,1
	
	def count_movies(self, pair_of_movies, value):
		yield (pair_of_movies , sum(value))



	def jacard_reducer(self, pair_of_movies, value):
		movies_in_common = value.next()	
		movie1 = pair_of_movies[0][1]
		movie2 = pair_of_movies	[1][1]
		
		jacard = movies_in_common /abs( movie1 + movie2 - movies_in_common)
		if jacard > 0.5:
			yield None , pair_of_movies

	
	def steps(self):
		return[
			MRStep(mapper = self.mapper, reducer = self.reducer2),
			MRStep( reducer = self.count_user_movies),
			MRStep( reducer = self.reducer2),
			MRStep( reducer = self.join_movies),
			MRStep( reducer = self.count_movies),
			MRStep( reducer = self.jacard_reducer)
		]





if __name__ == '__main__':
    MRWordFrequencyCount.run()

