# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import json

def createCombiner(value):
  neg = 0
  score = value[1]
  if value[0].lower() == "negative":
    neg = 1
    score = -value[1]
  return (1, neg, score)

def mergeValue(x, value):
  neg = 0
  score = value[1]
  if value[0].lower() == "negative":
    neg = 1
    score = -value[1]
  return (x[0] + 1, x[1] + neg, x[2] + score)

def mergeCombiners(x, y):
  return (x[0] + y[0], x[1] + y[1], x[2] + y[2])

def average(rdd):
  sumAndCount = rdd.map(lambda (key, value): value[0]).map(lambda x: (x, 1)).fold((0, 0), (lambda x, y: (x[0] + y[0], x[1] + y[1])))
  return float(sumAndCount[0]) / float(sumAndCount[1])

def filter(item, avg, percentage_f):
  if item[0] < avg:
    return False
  if item[1] / item[0] * 100 > percentage_f:
    return False
  return True
  

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, result_dir, percentage_f):
    inputRDD = sc.textFile(dataset_dir) 
    #p1
    entries = inputRDD.map(lambda x: json.loads(x))
    entries = entries.map(lambda x: (x['cuisine'], (x['evaluation'], x['points'])))
    entries = entries.combineByKey(createCombiner, mergeValue, mergeCombiners)
    #p2
    avg = average(entries)
    print(avg)
    print(percentage_f)
    #p3
    entries = entries.filter(lambda (key, value): filter(value, avg, percentage_f))
    #p4
    entries = entries.map(lambda (key, (tot, neg, score)): (key, (tot, neg, score, score / tot)))
    entries = entries.sortBy(lambda (key, (tot, neg, score, rat)): rat, ascending= False)
    entries.saveAsTextFile(result_dir)
    pass

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We provide the path to the input folder (dataset) and output folder (Spark job result)
    source_dir = "/FileStore/tables/A02/my_dataset/"
    result_dir = "/FileStore/tables/A02/my_result/"

    # 2. We add any extra variable we want to use
    percentage_f = 10

    # 3. We remove the monitoring and output directories
    dbutils.fs.rm(result_dir, True)

    # 4. We call to our main function
    my_main(source_dir, result_dir, percentage_f)
