"""
:organization: Logilab
:copyright: 2012 LOGILAB S.A. (Paris, FRANCE), license is LGPL v2.
:contact: http://www.logilab.fr/ -- mailto:contact@logilab.fr
:license: GNU Lesser General Public License, v2.1 - http://www.gnu.org/licenses
"""
import sys

if sys.platform == 'win32':
    CONDOR_ROOT = 'c:/condor'
else:
    CONDOR_ROOT = ''

options = (
    ('condor-root',
     {'type' : 'string',
      'default': CONDOR_ROOT,
      'help': 'Path to the root directory of the Condor '
          'installation. E.g. on Windows if Condor was installed to '
          '"%(root)s", the value should be "%(root)s" (default value on '
          'Windows). Use an empty string if the Condor binaries can be '
          'found using the PATH environment variable (default value on '
          'Unix), otherwise, the binaries are looked for in '
          '"$(condor-root)/bin".'  % {'root': CONDOR_ROOT},
      'group': 'condor',
      'level': 3,
      }),
    )
