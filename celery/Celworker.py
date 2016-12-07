from celery import Celery
import sys
import csv
import time
import re
from sets import Set
from functools import partial

app = Celery('item ', backend='rpc://', broker='amqp://test:test@192.168.2.244')

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

def createWordList(fileName):

  dataset = Set()
  with open(fileName,'r') as words:
    for line in words:
      for word in line.split():
        dataset.add(word.lower())
  return dataset



@app.task
def startWorker(item,wordlistPath,orgName):
    #threads = multiprocessing.cpu_count()
    #pool = Pool(processes=threads,)
    dataset = createWordList(wordlistPath)
    print "Starting map..."
    single_count_tuples = [Map(dataset,orgName,item)] #pool.map(partial(Map, dataset,orgName),items)
      
    print "Starting Partition..."
  
    # Organize the count tuples; lists of tuples by token key
    token_to_tuples = Partition(single_count_tuples)
  
    print "Starting Reduce..."
    # Collapse the lists of tuples into total term frequencies
    
    term_frequencies = map(Reduce,token_to_tuples.items())#pool.map(Reduce, token_to_tuples.items())

    
    print "Sorting list..."
    # Sort the term frequencies in nonincreasing order
    
    term_frequencies.sort (tuple_sort)
    
    return term_frequencies
