import argparse
import sys
import re
from pyspark import SparkContext, SparkConf
from operator import add

numLab = 0
labIndex = {}
labelGraph = None
l = []

def create_parser():
  parser = argparse.ArgumentParser()
  parser.add_argument('--iterations', type=int, default=2,
                      help='Number of iterations of label propagation')
  parser.add_argument('--edges_file', default=None,
                      help='Input file of edges')
  parser.add_argument('--seeds_file', default=None,
                      help='File that contains labels for seed nodes')
  parser.add_argument('--eval_file', default=None,
                      help='File that contains labels of nodes to be evaluated')
  parser.add_argument('--number_of_excutors', type=int, default=8,
                      help='Number of iterations of label propagation')
  return parser

def parseGraph(ep_edge):
  tokens = re.split(r'\t+', ep_edge)
  return tokens[0].strip(), (tokens[1].strip(), tokens[2].strip())

def parseGraph2(ep_edge):
  tokens = re.split(r'\t+', ep_edge)
  return tokens[1].strip(), (tokens[0].strip(), tokens[2].strip())

def parseTup(tup):
  iterObj = tup[1]
  sumW = 0
  for innerTup in iterObj:
    sumW = sumW + int(innerTup[1])

  l = []
  for innerTup in iterObj:
    l.append((innerTup[0], float(innerTup[1]) / sumW))


  return (tup[0], l)
  # sumW = iterObj.mapValues(sum)#lambda innerTup : innerTup[1]).sum()
  # print sumW
  # keyVal.dict().map(lambda val: val[1]).sum()

def f(val):
  return val

def iterate(iterable):
    r = []
    for v1_iterable in iterable:
        print v1
        for v2 in v1_iterable:
            print v2
            r.append(v2)

    return tuple(r)

def parseSeed(seed):
  seeds = re.split(r'\t+', seed)
  return seeds[0].strip(), seeds[1].strip()

def lab(seed):
  seeds = re.split(r'\t+', seed)
  return seeds[1].strip()

def preprocess(entity, labList):
  l = [0.0] * numLab
  for label in labList:
    l[labIndex[label]] = 1.0
  return (entity, l)

def computeContribs(tup, labList):
  if labList is None:
    return (tup[0], [0.0] * numLab)
  tmpList = [labList[i]*float(tup[1]) for i in range(len(labList))]
  return (tup[0], tmpList)

def normalizeLab(labList):
  sumL = sum(labList)
  if sumL == 0:
    return labList
  return [labList[i]/float(sumL) for i in range(len(labList))]

def addVectors(v1, v2):
    if v2 is None:
      return v1
    return map(sum, zip(v1, v2))

def replaceSeed(seedEntity, seedLDict, newList):
  if seedEntity in seedLDict:
    return seedLDict[seedEntity]
  return newList

def parseLabG(source, labels, entityList):
  if source in entityList:
    return (source, labels)

def labelMap(probList, labels):
  return tuple([x for _,x in sorted(zip(probList,labels), reverse=True)])

def parseResults(resultList):
  for x in resultList:
    # print x[0].strip().encode('utf-8'),
    # for lab in x[1]:
    #   print lab.strip().encode('utf-8'),
    #   # print "\t" + lab.encode('utf-8').strip(),
    # print

    sys.stdout.write(x[0].encode('utf-8') + "\t")
    labs=[word.encode('utf-8') for word in x[1]]
    print "\t".join(labs)
    # rowTup = tuple([word.encode('utf8') for word in x])
    # print ("\t").join(rowTup)

class LabelPropagation:
    def __init__(self, graph_file, seed_file, eval_file, iterations, number_of_excutors):
        conf = SparkConf().setAppName("LabelPropagation")
        conf = conf.setMaster('local[%d]'% number_of_excutors)\
                 .set('spark.executor.memory', '3G')\
                 .set('spark.driver.memory', '3G')\
                 .set('spark.driver.maxResultSize', '3G')
        self.spark = SparkContext(conf=conf)
        self.graph_file = graph_file
        self.seed_file = seed_file
        self.eval_file = eval_file
        self.n_iterations = iterations
        self.n_partitions = number_of_excutors * 2

    def run(self):

        global numLab
        global labIndex
        global labelGraph
        global l
        lines = self.spark.textFile(self.graph_file, self.n_partitions)
        
        # [TODO]
        links = lines.map(lambda ep_edge: parseGraph(ep_edge)).distinct().groupByKey().cache()
        reverselinks = lines.map(lambda ep_edge: parseGraph2(ep_edge)).distinct().groupByKey().cache()

        links = links.map(lambda tup: parseTup(tup))
        reverselinks = reverselinks.map(lambda tup: parseTup(tup))

        flat_Links = links.flatMapValues(f)
        flat_Reverselinks = reverselinks.flatMapValues(f)
        # print flat_Links.count()
        # print flat_Reverselinks.count()

        mergedLinks = flat_Links.union(flat_Reverselinks)
        # print mergedLinks.count()
        # links.collect()
        #print type(links) #RDD
        #print type(links.collect()[:2][1]) #Tuple
        # print type(links.collect()[:2][1][0]) #Unicode / Entity

        # print list(links.collect()[:2][1][1]) #Tuple

        # print type(links.collect()[:2][1][1]) #pyspark.resultiterable.ResultIterable
        # print type(links.collect()[:2][1][1]) #
        # normalize = links.map(lambda keyVal: parsekV(keyVal))
        # normalize.collect()[:1]

        lines = self.spark.textFile(self.seed_file, self.n_partitions)
        
        # [TODO]
        labels = lines.map(lambda entSeed : lab(entSeed)).distinct().sortBy(lambda x : x).cache()
        l = labels.collect()
        labIndex = dict(map(lambda lab : (lab, l.index(lab)), l))
        numLab = len(l)

        # print labIndex
        # print labels.collect()

        seedL = lines.map(lambda entSeed: parseSeed(entSeed)).distinct().groupByKey().cache()
        # print seedL.collect()[:10]
        seedL = seedL.map(lambda x : preprocess(x[0], x[1]))
        seedLDict = seedL.collectAsMap()
        # print seedL.collect()

        tmpMergedLinks = mergedLinks
        labelGraph = tmpMergedLinks.fullOuterJoin(seedL)
        # print labelGraph.collect()[:10]
        for t in range(self.n_iterations):
            # [TODO]
          # print labelGraph.collect()[800000:960000]
          contribs = labelGraph.map(lambda instance: computeContribs(instance[1][0], instance[1][1]))
          # print contribs.collect()[800000:960000]
          ranks = contribs.reduceByKey(addVectors).mapValues(lambda labList: normalizeLab(labList))
          nodeListDict = ranks.collectAsMap()
          # print nodeListDict
          labelGraph = labelGraph.map(lambda instance : (instance[0], (instance[1][0], addVectors(nodeListDict[instance[0]], instance[1][1])) ))
          labelGraph = labelGraph.map(lambda instance : (instance[0], (instance[1][0],  replaceSeed(instance[0], seedLDict, instance[1][1])) ) )

    def eval(self):
        global labelGraph
        global l
        lines = self.spark.textFile(self.eval_file, self.n_partitions)
        # entity = lines.map(lambda entSeed : entSeed).cache()
        # print lines.collect()
        # entityList = entity.collect()
        # results = labelGraph.map(lambda instance: parseLabG(instance[0], instance[1][1], entityList))
        # print lines.distinct().count()
        results = lines.map(lambda entity: (entity, 1)).join(labelGraph.map(lambda instance: (instance[0], instance[1][1])))
        results = results.map(lambda instance: (instance[0], labelMap(instance[1][1], l))).distinct()
        # print results.count()
        # print results
        # results = results.map(lambda instance : (instance[0], instance[1][1]))
        parseResults(results.collect())

        
        # [TODO]




if __name__ == "__main__":
    args = create_parser().parse_args()
    lp = LabelPropagation(args.edges_file, args.seeds_file, args.eval_file, args.iterations, args.number_of_excutors)
    lp.run()
    lp.eval()
    lp.spark.stop()
