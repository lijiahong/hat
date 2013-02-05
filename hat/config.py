#-*- coding: utf-8 -*-

import os
import os.path

hadoop_home_env = os.environ.get('HADOOP_HOME')
HADOOP_HOME = hadoop_home_env if hadoop_home_env else '~/hadoop/'

HADOOP_BIN = 'bin/hadoop'
HADOOP_BIN = os.path.join(HADOOP_HOME, HADOOP_BIN)

HADOOP_STREAMING_JAR = 'contrib/streaming/hadoop-*streaming*.jar'
HADOOP_STREAMING_JAR = os.path.join(HADOOP_HOME, HADOOP_STREAMING_JAR)
