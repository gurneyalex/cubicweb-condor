"""
condor job management utilities
"""

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

if sys.platform == 'win32':
    for command in CONDOR_COMMAND:
        CONDOR_COMMAND[command] += '.exe'

MISSING_COMMANDS_SIGNALED = set()

def get_scratch_dir():
    """
    return the condor scratch dir.

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

def status(config):
    """
    runs condor_status and return exit code and output of the command
    """
    status_cmd = osp.join(get_condor_bin_dir(config),
                          CONDOR_COMMAND['status'])
    return _simple_command_run([status_cmd])

def queue(config):
    """
    runs condor_queue and return exit code and output of the command
    """
    q_cmd = osp.join(get_condor_bin_dir(config),
                     CONDOR_COMMAND['queue'])
    return _simple_command_run([q_cmd])

def remove(config, jobid):
    """
    runs condor_remove and return exit code and output of the command
    """
    rm_cmd = osp.join(get_condor_bin_dir(config),
                      CONDOR_COMMAND['remove'])
    return _simple_command_run([rm_cmd, jobid])

def write_submit_file(command_args, job_params, operation):
    '''
    Write the submit description file for a job and returns the path to it
    '''
    quoted_args = argument_list_quote(command_args)
    job_params['arguments'] = quoted_args
    job_params['executable'] = SYS_EXECUTABLE
    job = CONDOR_PYTHON_JOB_TEMPLATE % job_params

    # write submit file on the fileserver
    filepath = osp.join(job_params['workingdirectory'], 
                        '%s.%s.submit' % (job_params['name'], operation))
    if osp.isfile(filepath):
        os.remove(filepath)
    with open(filepath, 'w') as submit_file:
        submit_file.write(job)
    
    return filepath

def submit(config, command_args, job_params):
    """
    submit a (python) job to condor with condor_submit and return exit
    code and output of the command

    config is passed to get the condor root directory
    command_args is a list of arguments passed to Python
    job_params is a dictionnary containing the following keys:
    * logfile
    * stderr
    * workingdirectory
    """
    SUBMIT_LOCK.acquire()
    try:
        submit_cmd = osp.join(get_condor_bin_dir(config),
                                 CONDOR_COMMAND['submit'])
        quoted_args = argument_list_quote(command_args)
        job_params['arguments'] = quoted_args
        job_params['executable'] = SYS_EXECUTABLE
        job = CONDOR_PYTHON_JOB_TEMPLATE % job_params

        pipe = subprocess.Popen([submit_cmd], stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pipe.stdin.write(job)
        pipe.stdin.close()
        output = pipe.stdout.read()
        status = pipe.wait()
        return status, output
    except OSError, exc:
        return - 1, str(exc)
    finally:
        SUBMIT_LOCK.release()

def submit_dag(config, dag_file):
    """
    submit dag of (python) jobs to condor and return exit code and
    output of the command
    
    config is passed to get the condor root directory dag_file is the
    path to the dag file
    """
    SUBMIT_LOCK.acquire()
    try:
        submit_cmd = osp.join(get_condor_bin_dir(config),
                                 CONDOR_COMMAND['dag'])

        pipe = subprocess.Popen(args=(submit_cmd, '-force', dag_file),
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT)

        output = pipe.stdout.read()
        status = pipe.wait()
        return status, output
    except OSError, exc:
        return - 1, str(exc)
    finally:
        SUBMIT_LOCK.release()


CONDOR_PYTHON_JOB_TEMPLATE = \
'''Universe=vanilla
Executable=%(executable)s
Arguments=%(arguments)s
Transfer_executable = False
Run_as_owner=True
InitialDir=%(workingdirectory)s
Log=%(logfile)s
Error=%(stderr)s
getenv=True
Queue
'''

def argument_quote(argument):
    """
    return the argument quoted according to the new arguments syntax of condor.
    See http://www.cs.wisc.edu/condor/manual/v7.4/2_5Submitting_Job.html for details.
    """
    argument = argument.replace('"', '""')
    if ' ' in argument:
        argument = argument.replace("'", "''")
        argument = "'" + argument + "'"
    return argument

def argument_list_quote(arguments):
    """
    quote eache argument in the list of argument supplied
    """
    args = []
    for arg in arguments:
        args.append(argument_quote(arg))
    return '"%s"' % ' '.join(args)

def get_condor_bin_dir(config):
    """
    return the directory where the condor executables are installed
    """
    condor_root = config['condor-root']
    if condor_root:
        return osp.join(condor_root, 'bin')
    else:
        return ''


def job_ids(config):
    '''
    return a list of job ids in the condor queue (as strings)
    '''
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

# the suspicious jobs list
__CANDIDATES = set()

def cleanup_stale_run_executions(repo):
    """
    clear RunExecutions which are supposed to be running or waiting in
    the Condor queue, but are not reported as such by Condor.

    The cleanup is done in two passes to avoid race conditions:
    
    * RE as tagged as suspicious if the condor queue is empty and the
      job should be there
    * a RE already tagged as suspicious, if still in the same
      situation the next time is transitioned to the failed state
    """
    global __CANDIDATES
    SUBMIT_LOCK.acquire()
    session = repo.internal_session()
    try:
        jobs = job_ids(repo.config)
        if jobs:
            logger.debug('Jobs waiting in queue, clearing suspicious job list')
            __CANDIDATES.clear()
            return
        if __CANDIDATES:
            logger.info('Suspicious RunExecutions: %s', __CANDIDATES)
        # if there are no job in the queue
        rql = "Any X WHERE X is RunExecution, X in_state S, S name in ('re_condor_queued', 're_condor_running')"
        for execution in session.execute(rql).entities():
            if execution.eid in __CANDIDATES:
                __CANDIDATES.remove(execution.eid)
                logger.error('cleanup_stale_run_executions: forcing transition re_fail on RunExecution %d', execution.eid)
                execution.fire_transition('re_fail', 'something failed, probably in Condor')
            else:
                logger.info('cleanup_stale_run_executions: found suspicious RunExecution %d', execution.eid)
                __CANDIDATES.add(execution.eid)
        session.commit()
    finally:
        SUBMIT_LOCK.release()
        session.close()

def _simple_command_run(cmd):
    if not osp.isfile(cmd[0]):
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


