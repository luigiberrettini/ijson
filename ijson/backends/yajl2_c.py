#
# Contributed by Rodrigo Tobar <rtobar@icrar.org>
#
# ICRAR - International Centre for Radio Astronomy Research
# (c) UWA - The University of Western Australia, 2016
# Copyright by UWA (in the framework of the ICRAR)
# All rights reserved
#
'''
Wrapper for _yajl2 C extension module
'''
import decimal

from ijson import common
from ._yajl2 import basic_parse as _basic_parse  # @UnresolvedImport
from ._yajl2 import parse as _parse  # @UnresolvedImport
from ._yajl2 import items as _items  # @UnresolvedImport

def basic_parse(file, **kwargs):
    return _basic_parse(file.read, decimal.Decimal, common.JSONError, common.IncompleteJSONError, **kwargs)

def parse(file, **kwargs):
    return _parse(file.read, decimal.Decimal, common.JSONError, common.IncompleteJSONError, **kwargs)

def items(file, prefix):
    return _items(prefix, file.read, decimal.Decimal, common.JSONError, common.IncompleteJSONError)
