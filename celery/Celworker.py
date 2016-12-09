from celery import Celery
import sys
import csv
import time
import re
from sets import Set
from functools import partial

#Configure broker access
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
        reader = csv.reader(csvfile, delimiter='\t')
        for row in reader:
            
            arr = []
            reader1 = csv.reader(row, delimiter=';')
            for l in reader1:
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
Combine tuples with the same key.
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
 
'''
Count the number of occurences for each (key,value) tuple
'''
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
    #Currently stored local, but could be moved to a cloud environment
    dataset = createWordList(wordlistPath)
    print "Starting map..."
    single_count_tuples = [Map(dataset,orgName,item)] 
      
    print "Starting Partition..."
  
    
    token_to_tuples = Partition(single_count_tuples)
  
    print "Starting Reduce..."
    
    
    term_frequencies = map(Reduce,token_to_tuples.items())
    
    print "Sorting list..."
    
    
    term_frequencies.sort (tuple_sort)
    
    return term_frequencies #Send back result to master
