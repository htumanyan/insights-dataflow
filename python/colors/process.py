import tfidf
import sys
import purestemmer as Stemmer
import re



documents_file =  sys.argv[1]
queries_file =  sys.argv[2]


shitty_descriptors=['metallic', 'and']


pattern = re.compile('\W')

def tokenize(document):
	return [ stemmer.stemWord(re.sub(pattern, '',  word)) for word in document[0:-1].lower().split(' ') if word not in shitty_descriptors]

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
		print ':%s,%s'%(query[:-1], top_match[0])
	else:
		no_match.append(query[:-1])

#print '%%%%%STATS%%%%'
#print 'total:%d, has_matched:%d, %f '%(total, has_match, has_match/total*100)

#print "%%%%NO MATCHES FOR%%%%"
#for el in no_match:
#	print  el



