"""
condor job management utilities
"""
from __future__ import with_statement

import sys
import logging
import os
import os.path as osp
import subprocess
import tempfile
from threading import Lock

SUBMIT_LOCK = Lock()
logger = logging.getLogger('cubicweb.condor')

if "service"  in sys.executable.lower():
    SYS_EXECUTABLE = r"C:\Python26\Python.exe"
else:
    SYS_EXECUTABLE = sys.executable

CONDOR_COMMAND = {'submit': 'condor_submit',
                  'dag': 'condor_submit_dag',
                  'queue': 'condor_q',
                  'remove': 'condor_rm',
                  'status': 'condor_status',
                  }

# XXX dubious
if sys.platform == 'win32':
    for command in CONDOR_COMMAND:
        CONDOR_COMMAND[command] += '.exe'

MISSING_COMMANDS_SIGNALED = set()

DEFAULT_JOB_PARAMS = {'Universe': 'vanilla',
                      'Transfer_executable': 'False',
                      'Run_as_owner': 'True',
                      'Executable': SYS_EXECUTABLE,
                      'getenv': 'True',
                      }

def get_scratch_dir():
    """ return the condor scratch dir.

    this is the directory where the job may place temporary data
    files. This directory is unique for every job that is run, and
    it's contents are deleted by Condor when the job stops running on
    a machine, no matter how the job completes.

    If the program is not running in a condor job, returns tempfile.gettempdir()
    """
    try:
        return os.environ['_CONDOR_SCRATCH_DIR']
    except KeyError:
        return tempfile.gettempdir()

def pool_debug(config):
    """
    determine which server is used for credd authentication
    as well as the DOMAIN_UID
    """

    args = ['-f', r'%s\t', 'Name',
            '-f', r'%s\t', 'uiddomain',
            '-f', r'%s\n', r'ifThenElse(isUndefined(LocalCredd),"UNDEF",LocalCredd)']

    status_cmd = osp.join(get_condor_bin_dir(config),
                          CONDOR_COMMAND['status'])

    return _simple_command_run([status_cmd] + args)


def status(config):
    """ runs condor_status and return exit code and output of the command """
    status_cmd = osp.join(get_condor_bin_dir(config),
                          CONDOR_COMMAND['status'])
    return _simple_command_run([status_cmd])

def queue(config):
    """ runs condor_queue and return exit code and output of the command """
    q_cmd = osp.join(get_condor_bin_dir(config),
                     CONDOR_COMMAND['queue'])
    return _simple_command_run([q_cmd])

def remove(config, jobid):
    """ runs condor_remove and return exit code and output of the command """
    rm_cmd = osp.join(get_condor_bin_dir(config),
                      CONDOR_COMMAND['remove'])
    return _simple_command_run([rm_cmd, jobid])

def build_submit_params(job_params):
    """ build submit params for a condor job submission
    with keys and values from a dictionary  """
    submit_params = DEFAULT_JOB_PARAMS.copy()
    submit_params.update(job_params)
    lines = ['%s=%s' % (k, v) for k, v in submit_params.iteritems()]
    lines.append('Queue')
    return '\n'.join(lines)

def submit(config, job_params):
    """ submit a (python) job to condor with condor_submit and return exit
    code and output of the command

    config is passed to get the condor root directory
    job_params should be built through build_submit_params method
    """
    with SUBMIT_LOCK:
        try:
            condor_submit_cmd = osp.join(get_condor_bin_dir(config),
                                         CONDOR_COMMAND['submit'])
            pipe = subprocess.Popen([condor_submit_cmd],
                                    stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT)
            pipe.stdin.write(job_params)
            pipe.stdin.close()
            output = pipe.stdout.read()
            status = pipe.wait()
            return status, output
        except OSError, exc:
            return -1, str(exc)

def submit_dag(config, dag_file):
    """ submit dag of (python) jobs to condor and return exit code and
    output of the command
    config is passed to get the condor root directory dag_file is the
    path to the dag file """
    with SUBMIT_LOCK:
        try:
            condor_dag_cmd = osp.join(get_condor_bin_dir(config),
                                      CONDOR_COMMAND['dag'])

            pipe = subprocess.Popen(args=(condor_dag_cmd, '-force', dag_file),
                                    stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT)

            output = pipe.stdout.read()
            status = pipe.wait()
            return status, output
        except OSError, exc:
            return -1, str(exc)

def argument_quote(argument):
    """ return the argument quoted according to the new arguments syntax of condor.
    See http://www.cs.wisc.edu/condor/manual/v7.4/2_5Submitting_Job.html for details. """
    argument = argument.replace('"', '""')
    if ' ' in argument:
        argument = argument.replace("'", "''")
        argument = "'" + argument + "'"
    return argument

def argument_list_quote(arguments):
    """ quote eache argument in the list of argument supplied """
    args = []
    for arg in arguments:
        args.append(argument_quote(arg))
    return '"%s"' % ' '.join(args)

def get_condor_bin_dir(config):
    """ return the directory where the condor executables are installed """
    condor_root = config['condor-root']
    if condor_root:
        return osp.join(condor_root, 'bin')
    else:
        return ''

def job_ids(config):
    """ return a list of job ids in the condor queue (as strings) """
    errcode, output = queue(config)
    interested = False
    ids = []
    if errcode != 0:
        return ids
    for line in output.splitlines():
        line = line.strip()
        if interested:
            if not line:
                break
            ids.append(line.split()[0])
        else:
            if line.startswith('ID'):
                interested = True
            continue
    logger.debug('found the following jobs in Condor queue: %s', ids)
    return ids


def _simple_command_run(cmd):
    if not osp.isfile(cmd[0]):
        if sys.platform == 'win32':
            if cmd[0] not in MISSING_COMMANDS_SIGNALED:
                MISSING_COMMANDS_SIGNALED.add(cmd[0])
                logger.error('Cannot run %s. Check condor installation and '
                             'instance configuration' % cmd[0])
            return -1, u'No such file or directory %s' % cmd[0]
    try:
        pipe = subprocess.Popen(cmd,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT)
        output = pipe.stdout.read()
        errcode = pipe.wait()
        logger.debug('%s exited with status %d', cmd, errcode)
        if errcode != 0:
            logger.error('error while running %s: %s', cmd, output)

        return errcode, output.decode('latin-1', 'replace')
    except OSError, exc:
        logger.exception('error while running %s', cmd)
        return -1, str(exc).decode('latin-1', 'replace')


