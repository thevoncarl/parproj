import sys
from multiprocessing import Pool
import csv
import time
import re
from sets import Set
from functools import partial

  
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
Find all files in a directory and returns to path to each one of them
"""

def findFiles(directory):
  from os import listdir
  from os.path import isfile, join
  files = [directory + f for f in listdir(directory) if isfile(join(directory, f))]
  return files
  
"""
Find occurences of words in for every entry text that occurs in our wordList

"""
def Map(dataset,org,fileName):
    results = []
    with open(fileName,'rU') as csvfile:
        spamreader = csv.reader(csvfile, delimiter='\t')
        for row in spamreader:
            
            arr = []
            spamreader = csv.reader(row, delimiter=';')
            for l in spamreader:
                arr.append(str(l))
            company = (arr[2])[2:-2] #arr[2] is where the post originates from 
            
            if company  == org: 
              for word in arr[7].split(): #arr[7] is corresponding to the text contained in the post
                #Simple word processing
                if word.isalpha():
                  if str(word).lower() in dataset:
                    results.append((word.lower(),1))
              
            
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

"""
Based on a file with words for each line, create and return a dictionary
"""
def createWordList(fileName):

  dataset = Set()
  with open(fileName,'r') as words:
    for line in words:
      for word in line.split():
        dataset.add(word.lower())
  return dataset


if __name__ == '__main__':
  
  if (len(sys.argv) != 5):
    print "Usage: python mapred.py [Input directory] [Path to wordlist file] [Name of company to find post from] [Nr of threads for map & reduce] \nExample: python mapred.py \'Input/\' \'sports\' \'Twitter\' 4"
    sys.exit(1)

  inDir = sys.argv[1]
  wordDir = sys.argv[2]
  orgName = sys.argv[3]
  procc = int(sys.argv[4])
  start = time.time()
  
  pool = Pool(processes=procc,)
 
 
  dataset = createWordList(wordDir)
  print "Starting map..."

  files = findFiles(inDir)

  single_count_tuples = pool.map(partial(Map, dataset,orgName),files)
  
  
  print "Starting Partition..."
  
  # Organize the count tuples; lists of tuples by token key
  token_to_tuples = Partition(single_count_tuples)
  
  print "Starting Reduce..."
  # Collapse the lists of tuples into total term frequencies
  
  term_frequencies = pool.map(Reduce, token_to_tuples.items())

  
  print "Sorting list..."
  # Sort the term frequencies in nonincreasing order
   
  term_frequencies.sort (tuple_sort)
 
  for pair in term_frequencies[:20]:
      print pair[0], ":", pair[1]

  end = time.time()
  print(end - start)
