from Celworker import startWorker
import sys
from multiprocessing import Pool
import csv
import time
import re
from sets import Set
from functools import partial

"""
Find all files in a directory and returns to path to each one of them
"""

def findFiles(directory):
  from os import listdir
  from os.path import isfile, join
  files = [directory + f for f in listdir(directory) if isfile(join(directory, f))]
  return files

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



if __name__ == '__main__':
    if (len(sys.argv) != 5):
        print "Usage: python Master.py [Input directory] [Path to wordlist file] [Name of company to find post from] [Master threads] \nExample: python Master.py \'Input/\' \'sports\' \'Twitter\' 4"
        sys.exit(1)

    inDir = sys.argv[1]
    wordDir = sys.argv[2]
    orgName = sys.argv[3]
    procc = int(sys.argv[4])
    
    start = time.time()
    '''
    Currently import the files by looking locally, can however easily be expanded to retrieve filenames stored online such as in a cloud environment
    '''
    files = findFiles(inDir)

    workers = []
    for job in files:
        result = startWorker.delay(job,wordDir,orgName)
        workers.append(result)
    
    single_count_tuples = []
    while (len(workers) != 0):
      for task in workers:
        single_count_tuples.append(task.get())
        workers.remove(task)
        
    token_to_tuples = Partition(single_count_tuples)
    pool = Pool(processes=procc,)
    term_frequencies = pool.map(Reduce, token_to_tuples.items())
    
    term_frequencies.sort (tuple_sort)
 
    for pair in term_frequencies[:20]:
      print pair[0], ":", pair[1]

    
    end = time.time()
    print(end - start)
