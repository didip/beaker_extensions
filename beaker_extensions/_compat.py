# Excerpted from beaker._compat.py.

from __future__ import absolute_import
import sys
import six

# True if we are running on Python 2.
PY2 = sys.version_info[0] == 2


if not PY2:  # pragma: no cover

    def u_(s):
        return str(s)


else:
    unicode_text = six.text_type
    byte_string = str

    def u_(s):
        if isinstance(s, unicode_text):
            return s

        if not isinstance(s, byte_string):
            s = str(s)
        return six.text_type(s, "utf-8")
