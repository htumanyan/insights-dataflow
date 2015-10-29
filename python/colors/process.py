import tfidf
import sys
import purestemmer as Stemmer



documents_file =  sys.argv[1]
queries_file =  sys.argv[2]

def tokenize(document):
	return [ stemmer.stemWord(word) for word in document[0:-1].lower().split(' ')]

table = tfidf.tfidf()

stemmer =   Stemmer.Stemmer('english')
for document in open(documents_file):
	table.addDocument(document[:-1], tokenize(document))


total = 0
has_match = 0
no_match=[]
for query in open(queries_file):
	top_match =  sorted(table.similarities( tokenize(query)), key= lambda list:list[1], reverse=True)[0] 
	top_match_score = top_match[1]
	total = total+1
	if(top_match_score > 0):
		has_match = has_match +1;
		print 'original:%s chrome:%s'%(query[:-1], top_match[0])
	else:
		no_match.append(query[:-1])

print '%%%%%STATS%%%%'
print 'total:%d, has_matched:%d, %d '%(total, has_match, has_match/total)

print "%%%%NO MATCHES FOR%%%%"
for el in no_match:
	print  el



