#-*- coding: utf-8 -*-

import sys
import subprocess

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

from config import HADOOP_HOME, HADOOP_BIN

class CommandNotFoundError(Exception):
    """Raised when command is not found."""
    pass

class HadoopFS(object):
    def __init__(self, debug=False):
        self.debug = debug
        self.commands = ['cat', 'ls', 'put', 'mkdir', 'rm', 'rmr']
        self.output_commands = ['cat', 'ls']
    
    def _hadoop_command(self, command, *args):
        if args:
            hadoop_args = []
            for arg in args:
                arg = arg.split(' ')
                for a in arg:
                    hadoop_args.append(a)
            hadoop_args = ' '.join(hadoop_args)
        else:
            hadoop_args = ''
        hadoop_cmd = '%s %s -%s %s' % (HADOOP_BIN, 'fs', command, hadoop_args)
        process = subprocess.Popen(hadoop_cmd, 
                                   stdin=subprocess.PIPE, 
                                   stdout=subprocess.PIPE, 
                                   stderr=subprocess.PIPE, 
                                   shell=True)
        stdout, stderr = process.communicate()
        stdout = StringIO(stdout)
        stderr = StringIO(stderr)
        error_lines = stderr.readlines()
        if error_lines:
            if self.debug:
                for line in error_lines:
                    sys.stdout.write(line)
        output_lines = stdout.readlines()
        if command in self.output_commands:
            lines = []
            for line in output_lines:
                lines.append(line.strip())
            return lines
        elif command in self.commands:
            if error_lines:
                return -1
            else:
                return 1
        else:
            pass

    def __getattr__(self, attr, *args):
        if attr in self.commands:
            def warpper(*args):
                return self._hadoop_command(attr, *args)
            return warpper
        else:
            raise CommandNotFoundError('Command not found: %s' % command)
