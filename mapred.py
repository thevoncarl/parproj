import sys
from multiprocessing import Pool
import csv
import time
import re
import nltk
from nltk.corpus import wordnet as wn
from nltk.corpus.reader import NOUN


nouns = {x.name().split('.', 1)[0] for x in wn.all_synsets('v')}
"""
Sort tuples by term frequency, and then alphabetically.
"""

def tuple_sort (a, b):
  if a[1] < b[1]:
    return 1
  elif a[1] > b[1]:
    return -1
  else:
    return cmp(a[0], b[0])

  
"""
Given a list of tokens, return a list of tuples of
titlecased (or proper noun) tokens and a count of '1'.
Also remove any leading or trailing punctuation from
each token.
"""


def Map(L):
 
    results = []
    with open(L,'rU') as csvfile:
        spamreader = csv.reader(csvfile, delimiter='\t')
        i = 0
        for row in spamreader:
            
            arr = []
            spamreader = csv.reader(row, delimiter=';')
            for l in spamreader:
                arr.append(str(l))
            if arr[2] == "['Twitter']": #arr[2] != "[]" and  len(arr[2]) < 20:
              for word in arr[7].split():
                if word.isalpha():
                  if word in nouns:
                    results.append((word,1))
           
            
    return results

'''
Group the sublists of (token, 1) pairs into a term-frequency-list
map, so that the Reduce operation later can work on sorted
term counts. The returned result is a dictionary with the structure
{token : [(token, 1), ...] .. }
'''
def Partition(L):
    tf = {}
    for sublist in L:
        for p in sublist:
            # Append the tuple to the list in the map
            try:
                tf[p[0]].append (p)
            except KeyError:
                tf[p[0]] = [p]
    return tf
 
"""
Given a (token, [(token, 1) ...]) tuple, collapse all the
count tuples from the Map operation into a single term frequency
number for this token, and return a final tuple (token, frequency).
"""
def Reduce(Mapping):
  return (Mapping[0], sum(pair[1] for pair in Mapping[1]))



if __name__ == '__main__':

  
  if (len(sys.argv) != 2):
    print "Program requires path to file for reading!"
    sys.exit(1)
  start = time.time()
  
  pool = Pool(processes=4,)
 
  # Fragment the string data into 8 chunks
 
  print "map now"
  # Generate count tuples for title-cased tokens
  lists = ['./Tweets/e11','./Tweets/e12','./Tweets/e13','./Tweets/e14','./Tweets/e21','./Tweets/e22','./Tweets/e23','./Tweets/e24']
  #lists = ['entries1.csv','entries2.csv','entries3.csv']
  single_count_tuples = pool.map(Map, lists)
  

  print "partition now"
  # Organize the count tuples; lists of tuples by token key
  token_to_tuples = Partition(single_count_tuples)
  print "reduce now"
  # Collapse the lists of tuples into total term frequencies
  
  term_frequencies = pool.map(Reduce, token_to_tuples.items())
  print "sort now"
  # Sort the term frequencies in nonincreasing order

  
  
  term_frequencies.sort (tuple_sort)
 
  for pair in term_frequencies[:20]:
      print pair[0], ":", pair[1]

  end = time.time()
  print(end - start)
