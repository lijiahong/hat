#-*- coding: utf-8 -*-

import sys

from hat.job import Hat
from hat.fs import HadoopFS

class PageRankIter(Hat):
    def mapper(self, key, value):
        #in case of some bad inputs
        if not key.strip():
            return
        tokens = value.split(',')
        symbol = tokens[0]
        if symbol != 'pr_results':
            return
        current_pr = float(tokens[1])
        total_nodes = int(tokens[2])
        if len(tokens) > 3:
            outlinks = tokens[3:]
            L = len(outlinks)
            for outlink in outlinks:
                #make sure outlink is not empty
                if outlink.strip():
                    yield (outlink, '%s,%s,%s,%s' % ('pr', key, current_pr / L, total_nodes))
            #prepare for next iter
            yield (key, '%s,%s,%s' % ('outlinks', total_nodes, ','.join(outlinks)))
        else:
            #do not have outlinks
            yield (key, '%s,%s,%s,%s' % ('pr', key, current_pr, total_nodes))
            yield (key, '%s,%s' % ('outlinks', total_nodes))

    def reducer(self, key, values):
        alpha = 0.85
        pr_sum = 0.0
        outlinks = None
        total_nodes = None
        for value in values:
            tokens = value.split(',')
            symbol = tokens[0]
            if symbol == 'pr':
                pr_i = tokens[2]
                total_nodes = int(tokens[3])
                pr_sum += float(pr_i)
            if symbol == 'outlinks':
                total_nodes = int(tokens[1])
                try:
                    outlinks = tokens[2:]
                except IndexError:
                    outlinks = None
        rank = (1 - alpha) / total_nodes + alpha * pr_sum
        if outlinks:
            yield (key, '%s,%s,%s,%s' % ('pr_results', rank, total_nodes, ','.join(outlinks)))
        else:
            yield (key, '%s,%s,%s' % ('pr_results', rank, total_nodes))


class PageRankSorter(Hat):
    def mapper(self, key, value):
        tokens = value.split(',')
        symbol = tokens[0]
        if symbol != 'pr_results':
            return
        current_pr = '%.8f' % float(tokens[1])
        yield (current_pr, key)

    def reducer(self, key, values):
        for value in values:
            yield (value, key)
        
def main():
    job_id = 'hat_1'

    if (len(sys.argv) < 3):
        print 'Usage: python pagerank.py input_file iter_count'
        sys.exit()
    else:
        iter_count = int(sys.argv[2])
        input_file_name = sys.argv[1]

    fs = HadoopFS()
    #set work dir and put input file into file system
    fs.mkdir('%s' % job_id)
    fs.put(input_file_name, '%s/hat_init' % job_id)

    #init
    pr_iter = PageRankIter(input_path='%s/hat_init' % job_id, output_path='%s/hat_tmp1' % job_id)
    pr_iter.run()

    #iter
    for i in range(iter_count-1):
        pr_iter = PageRankIter(input_path='%s/hat_tmp%s' % (job_id, (i+1)), output_path='%s/hat_tmp%s' % (job_id, (i+2)))
        pr_iter.run()

    #sort
    pr_sorter = PageRankSorter(input_path='%s/hat_tmp%s' % (job_id, iter_count), output_path='%s/hat_results' % job_id)
    pr_sorter.run()

    #output and clean work dir
    try:
        outputs = fs.cat('%s/hat_results/*' % job_id)
        if len(outputs) > 100:
            outputs = outputs[-100:]
        for line in outputs:
            print line
    except Exception:
        raise
    finally:
        fs.rmr('%s' % job_id)

if __name__ == '__main__': main()
