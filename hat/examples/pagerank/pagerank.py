#-*- coding: utf-8 -*-

from hat.job import Hat

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
        try:
            outlinks = tokens[3:]
            L = len(outlinks)
            if L > 0:
                for outlink in outlinks:
                    #make sure outlink is not empty
                    if outlink.strip():
                        yield (outlink, '%s,%s,%s,%s' % ('pr', key, current_pr / L, total_nodes))
                yield (key, '%s,%s,%s' % ('outlinks', total_nodes, ','.join(outlinks)))
        except IndexError:
            #do not have outlinks
            yield (key, '%s,%s' % ('outlinks', total_nodes))
        

    def reducer(self, key, values):
        alpha = 0.85
        rank = 0.0
        outlinks = None
        total_nodes = None
        for value in values:
            tokens = value.split(',')
            symbol = tokens[0]
            if symbol == 'pr':
                pr_i = tokens[2]
                total_nodes = int(tokens[3])
                rank += float(pr_i)
            if symbol == 'outlinks':
                total_nodes = int(tokens[1])
                try:
                    outlinks = tokens[2:]
                except IndexError:
                    outlinks = None
        rank = (1 - alpha) / total_nodes + alpha * rank
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
    iter_count = 2

    #init
    pr_iter = PageRankIter(input_path='pr_init', output_path='pr_tmp1')
    pr_iter.run()

    #iter
    for i in range(iter_count-1):
        pr_iter = PageRankIter(input_path='pr_tmp%s' % (i+1), output_path='pr_tmp%s' % (i+2))
        pr_iter.run()

    #sort
    pr_sorter = PageRankSorter(input_path='pr_tmp%s' % iter_count, output_path='pr_results')
    pr_sorter.run()

if __name__ == '__main__': main()
