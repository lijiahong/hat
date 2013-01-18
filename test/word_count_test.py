#-*- coding: utf-8 -*-

import sys
sys.path.append('../hat/')

from hat.job import Hat

class WordCountHat(Hat):
    def mapper(self, key, value):
        if not value:
            value = 1
        keys = key.split()
        for key in keys:
            yield (key, value)

    def reducer(self, key, values):
        yield (key, sum(map(int, values)))

def test_inspect(hat):
    hat._make_mapper_file()
    hat._make_reducer_file()

def test_run_hadoop(hat):
    hat.run()

def main():
    word_count_hat  = WordCountHat(input_path='input')
    test_run_hadoop(word_count_hat)


if __name__ == '__main__': main()
