
from mrjob.job import MRJob
from mrjob.step import MRStep
#from mrjob.protocol import JSONValueProtocol
from mr3px.csvprotocol import CsvProtocol


class MRWordFrequencyCount(MRJob):
       # INPUT_PROTOCOL = JSONValueProtocol
	INPUT_PROTOCOL = CsvProtocol
	OUTPUT_PROCOTOL = CsvProtocol
	def mapper(self, _, line):
	#	print line
		yield ['word', line[1]]
	

	
	def combiner_count_words(self, word, iterable):
		r=2
		pool = tuple(iterable)
  		n = len(pool)
    		if r > n:
        		return
    		indices = range(r)
    		yield tuple(pool[i] for i in indices),1
    		while True:
        		for i in reversed(range(r)):
            			if indices[i] != i + n - r:
                			break
        		else:
            			return
        		indices[i] += 1
        		for j in range(i+1, r):
            			indices[j] = indices[j-1] + 1
        		yield tuple(pool[i] for i in indices),1

		
	
	def reducer2(self,words,counter):
		yield (words,sum(counter))		
			

	def reducer3(self,word,count):
		yield ['max', (sum(count),word)]


	def maxReducer(self, _, word):
		yield max(word)	

	def steps(self):
                return [
                        MRStep(mapper=self.mapper,
			reducer = self.combiner_count_words),
			MRStep(reducer = self.reducer2),
			MRStep(reducer = self.reducer3),
			MRStep(reducer = self.maxReducer)
			
                         ]



if __name__ == '__main__':
    MRWordFrequencyCount.run()

		

