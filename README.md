hat
===

hat is a Python wrapper for Hadoop Streaming jobs just like [mrjob](https://github.com/yelp/mrjob/).
However, it is just a simplified implement for study and for fun.

Why named hat?
--------------
Inspired by the book [The Little Prince](http://en.wikipedia.org/wiki/The_Little_Prince), where the
little prince's first drawing was thought a hat by grown-ups, but actually a boa constrictor(Python)
digesting an elephant(Hadoop).

Installation
------------
``python setup.py install``

To run on Hadoop cluster, make sure ``$HADOOP_HOME`` is set or just put Hadoop into your home directory.

Example
-------
First, initialize input data:

``$ bin/hadoop fs -put ./test/little_prince_ch1.txt input``

Second, write a MapReduce job:

    from hat.job import Hat

    class WordCountHat(Hat):
        def mapper(self, key, value):
            #key is a line in text, and value is None
            if not value:
                value = 1
            keys = key.split()
            for key in keys:
                yield (key, value)

        def reducer(self, key, values):
            yield (key, sum(map(int, values)))

    def main():
        word_count_hat = WordCountHat(input_path='input', output_path='outout')
        word_count_hat.run()

    if __name__ == '__main__': main()

Third, checkout output:

``$ bin/hadoop fs -cat /output/*``