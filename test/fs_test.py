#-*- coding: utf-8 -*-

from hat.fs import HadoopFS

fs = HadoopFS(debug=True)

fs.put('test.txt', 'test_for_fs')

for line in fs.cat('test_for_fs/*'):
    print line

fs.rmr('test_for_fs')

for line in fs.ls():
    print line
