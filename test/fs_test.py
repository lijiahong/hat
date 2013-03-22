#-*- coding: utf-8 -*-

from hat.fs import HadoopFS

fs = HadoopFS(debug=True)

print fs.mkdir('test')

print fs.put('test.txt', 'test/test_for_fs')

for line in fs.cat('test/test_for_fs/*'):
    print line

print fs.rmr('test')

for line in fs.ls():
    print line
