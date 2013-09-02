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
    return _simple_command_run([q_cmd, '-global'],
                               ignore_output_errors=['All queues are empty',])

def remove(config, schedd, jobid):
    """ runs condor_remove and return exit code and output of the command """
    rm_cmd = osp.join(get_condor_bin_dir(config),
                      CONDOR_COMMAND['remove'])
    return _simple_command_run([rm_cmd, jobid, '-name', schedd])


def _build_environment(envdict):
    """ Weird-ish condor friendly env line builder
    See http://research.cs.wisc.edu/htcondor/manual/v8.0/condor_submit.html
    """
    lines = []
    for k, v in envdict.iteritems():
        if " " in v: # NOTE: per the spec, one might want to handle all 'whitespace' chars.
            v = v.replace("'", "''")
            v = "'%s'" % v
        v = v.replace('"', '""')
        lines.append('%s=%s'  % (k, v))
    return '"%s"' % ' '.join(lines)

def build_submit_params(job_params, use_submitter_env=True, more_environment=None):
    """build submit params for a condor job submission
    with keys and values from a dictionary

    By default, Condor will use the environment variables of the target machine and
    overwrite them by the ones present on the calling machine, except if
    you change use_submitter_env to False.

    You also can use the dictionnary more_environment to add (or
    overwrite) specific environment variables

    """
    submit_params = DEFAULT_JOB_PARAMS.copy()
    if use_submitter_env:
        submit_params['getenv'] = 'True'
    if more_environment:
        submit_params['environment'] = _build_environment(more_environment)
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
    """ return a list of job ids in the condor queue (as strings)
    The parsing is done for an output that looks like:

pagode@crater1:~/confs/pagode/cubes/condor$ condor_q -global


-- Schedd: foo.logilab.fr : <192.168.1.42:49452>
 ID      OWNER            SUBMITTED     RUN_TIME ST PRI SIZE CMD
   4.0   schabot        12/11 13:30   0+00:00:01 H  0   0.0  align.py name
   7.0   schabot        12/11 13:41   0+00:00:00 H  0   0.0  align.py
  12.0   schabot        12/11 13:50   0+00:00:01 H  0   0.0  sh_loop.py

5 jobs; 0 idle, 0 running, 5 held


-- Schedd: condor.logilab.fr : <192.168.1.142:54657>
 ID      OWNER            SUBMITTED     RUN_TIME ST PRI SIZE CMD
   6.0   logilab         6/20 10:28   0+00:00:00 I  0   0.0  simple 4 10
   7.0   logilab         6/20 10:40   0+00:00:00 I  0   0.0  simple 4 10

    """
    errcode, output = queue(config)
    parse_line = False
    current_sched = None
    ids = []
    if errcode != 0:
        logger.debug('queue command issued return code: %s', errcode)
        return ids

    for line in output.splitlines():
        line = line.strip()
        parse_line = parse_line and bool(line)
        if parse_line:
            assert current_sched
            ids.append( (current_sched, line.split()[0]) )
            continue

        if line.startswith('--'):
            current_sched = line.split()[2].strip()

        if line.startswith('ID'):
            parse_line = True

    logger.debug('found the following jobs in Condor queue: %s', ids)
    return ids


def _simple_command_run(cmd, ignore_output_errors=[]):
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
            # oddly, condor_q -global will exit with 127 if all queues are empty
            if not any(ignore in output
                       for ignore in ignore_output_errors):
                logger.error('error while running %s: %s', cmd, output)

        return errcode, output.decode('latin-1', 'replace')
    except OSError, exc:
        logger.exception('error while running %s', cmd)
        return -1, str(exc).decode('latin-1', 'replace')
