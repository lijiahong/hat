#-*- coding: utf-8 -*-

import sys
import os
import os.path
import codecs
import inspect
import select
import subprocess
import time

hadoop_home_env = os.environ.get('HADOOP_HOME')
HADOOP_HOME = hadoop_home_env if hadoop_home_env else '~/hadoop/'

HADOOP = 'bin/hadoop'

HADOOP_STREAMING_JAR = '%scontrib/streaming/hadoop-*streaming*.jar' % HADOOP_HOME

CODE_DIR = 'pycode'

DEFAULT_OUTPUT_PATH = 'output'

class InputPathError(Exception):
    """Raised when input path is not specified."""
    pass

class Hat(object):
    def __init__(self, **kargs):
        '''Hadoop Streaming arguments:
             -mapper: Mapper executable
             -recuder: Reducer executable
             -input: Input location for mapper
             -ouput: Output location for reducer
             -file: Make the mapper, reducer executable available locally on the compute nodes
        '''
        #task name
        if kargs.has_key('task_name'):
            self.task_name = kargs['task_name']
        else:
            self.task_name = 'job%s' % int(time.time())
        #input and output path
        if kargs.has_key('input_path'):
            self.input = kargs['input_path']
        else:
            raise InputPathError('Input path is not specified.')
        if kargs.has_key('output_path'):
            self.output = kargs['output_path']
        else:
            self.output = DEFAULT_OUTPUT_PATH
        #mapper and recuder executable code base dir
        if not os.path.exists(CODE_DIR):
            os.mkdir(CODE_DIR)

    def mapper(self, key, value):
        raise NotImplementedError

    def reducer(self, key, values):
        raise NotImplementedError

    def run(self):
        mapper_path = self._make_mapper_file()
        reducer_path = self._make_reducer_file()
        self.hadoop_stream_command('-input %s -output %s -mapper %s -file %s -reducer %s -file %s' % 
                                   (self.input, self.output, 
                                    mapper_path, mapper_path,
                                    reducer_path, reducer_path))

    def hadoop_stream_command(self, cmd_args):
        hadoop_cmd = '%s%s jar %s %s' % (HADOOP_HOME, HADOOP, HADOOP_STREAMING_JAR, cmd_args)
        print 'Execute: %s' % hadoop_cmd
        process = subprocess.Popen(hadoop_cmd, 
                                   stdin=subprocess.PIPE, 
                                   stdout=subprocess.PIPE, 
                                   stderr=subprocess.PIPE, 
                                   shell=True)
        #using select to obtain realtime output
        stdout_fd = process.stdout.fileno()
        stderr_fd = process.stderr.fileno()
        read_fds = [stdout_fd, stderr_fd]
        while True:
            ret = select.select(read_fds, [], [])
            for fd in ret[0]:
                if fd == stdout_fd:
                    sys.stdout.write(process.stdout.readline())
                elif fd == stderr_fd:
                    sys.stdout.write(process.stderr.readline())
            if process.poll() != None:
                break
        print 'Job <%s>: DONE' % self.task_name

    def _make_mapper_file(self):
        mapper_path = '%s/mapper.py' % CODE_DIR
        mapper_file = codecs.open(mapper_path, 'w', encoding='utf-8')
        mapper_str = "#!/usr/bin/env python\n"
        mapper_str += "#-*- coding: utf-8 -*-\n"
        mapper_str += "import sys\n"
        mapper_str += "def read_input(file):\n"
        mapper_str += "    for line in file:\n"
        mapper_str += "        yield line.rstrip()\n"
        mapper_str += "def main():\n"
        mapper_str += "    data = read_input(sys.stdin)\n"
        mapper_str += "    for line in data:\n"
        mapper_str += "        tokens = line.split('\\t', 1)\n"
        mapper_str += "        if len(tokens) == 1:\n"
        mapper_str += "            key, value = tokens[0], None\n"
        mapper_str += "        else:\n"
        mapper_str += "            key, value = tokens[0], tokens[1]\n"
        mapper_str += "        for _key, _value in mapper(key, value):\n"
        mapper_str += "            print '%s\\t%s' % (_key, _value)\n"
        mapper_str += "def mapper(key, value):\n"
        mapper_func_str = self._generate_code(self.mapper)
        for line in mapper_func_str[1:]:
            #remove a indent 
            mapper_str += line[4:]
        mapper_str += "if __name__ == '__main__': main()\n"
        mapper_file.write(mapper_str)
        mapper_file.close()
        return mapper_path

    def _make_reducer_file(self):
        reducer_path = '%s/reducer.py' % CODE_DIR
        reducer_file = codecs.open(reducer_path, 'w', encoding='utf-8')
        reducer_str = "#!/usr/bin/env python\n"
        reducer_str += "#-*- coding: utf-8 -*-\n"
        reducer_str += "import sys\n"
        reducer_str += "from itertools import groupby\n"
        reducer_str += "from operator import itemgetter\n"
        reducer_str += "def read_mapper_output(file):\n"
        reducer_str += "    for line in file:\n"
        reducer_str += "        yield line.rstrip()\n"
        reducer_str += "def main():\n"
        reducer_str += "    data = read_mapper_output(sys.stdin)\n"
        reducer_str += "    for key, group in groupby(data, key=lambda x: x.split('\\t')[0]):\n"
        reducer_str += "        values = (item.split('\\t')[1] for item in group)\n"
        reducer_str += "        for _key, _value in reducer(key, values):\n"
        reducer_str += "            print '%s\\t%s' % (_key, _value)\n"
        reducer_str += "def reducer(key, values):\n"
        reducer_func_str = self._generate_code(self.reducer)
        for line in reducer_func_str[1:]:
            #remove a indent 
            reducer_str += line[4:]
        reducer_str += "if __name__ == '__main__': main()\n"
        reducer_file.write(reducer_str)
        reducer_file.close()
        return reducer_path

    def _generate_code(self, py_object):
        #using inspect to obtain function source code on th fly
        return inspect.getsourcelines(py_object)[0]
