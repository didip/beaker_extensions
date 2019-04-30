# Excerpted from beaker._compat.py.

import sys

# True if we are running on Python 2.
PY2 = sys.version_info[0] == 2


if not PY2:  # pragma: no cover
    def u_(s):
        return str(s)

else:
    unicode_text = unicode
    byte_string = str

    def u_(s):
        if isinstance(s, unicode_text):
            return s

        if not isinstance(s, byte_string):
            s = str(s)
        return unicode(s, 'utf-8')
