# Copyright 2011-2012 GRNET S.A. All rights reserved.
#
# Redistribution and use in source and binary forms, with or
# without modification, are permitted provided that the following
# conditions are met:
#
#   1. Redistributions of source code must retain the above
#      copyright notice, this list of conditions and the following
#      disclaimer.
#
#   2. Redistributions in binary form must reproduce the above
#      copyright notice, this list of conditions and the following
#      disclaimer in the documentation and/or other materials
#      provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY GRNET S.A. ``AS IS'' AND ANY EXPRESS
# OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
# PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL GRNET S.A OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
# USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
# AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#
# The views and conclusions contained in the software and
# documentation are those of the authors and should not be
# interpreted as representing official policies, either expressed
# or implied, of GRNET S.A.

"""
Alembic migration wrapper for pithos backend database.

- Locate alembic.ini in backends/lib/sqlalchemy package and pass it
  as parameter to alembic

e.g::

    $ pithos-migrate upgrade head

"""

import sys
import os

from alembic.config import main as alembic_main
from pithos.backends.lib import sqlalchemy as sqlalchemy_backend

DEFAULT_ALEMBIC_INI_PATH = os.path.join(
        os.path.abspath(os.path.dirname(sqlalchemy_backend.__file__)),
        'alembic.ini')

def main(argv=None, **kwargs):
    if not argv:
        argv = sys.argv

    # clean up args
    argv.pop(0)

    # default config arg, if not already set
    if not '-c' in argv:
        argv.insert(0, DEFAULT_ALEMBIC_INI_PATH)
        argv.insert(0, '-c')


    alembic_main(argv, **kwargs)
if __name__ == '__main__':
    import sys
    main(sys.argv)

