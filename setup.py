import distutils.ccompiler
import distutils.sysconfig
from importlib import import_module
import os
import platform
import tempfile

from setuptools import setup, find_packages, Extension


setupArgs = dict(
    name = 'ijson',
    version = import_module('ijson').__version__,
    author = 'Ivan Sagalaev',
    author_email = 'maniac@softwaremaniacs.org',
    url = 'https://github.com/isagalaev/ijson',
    license = 'BSD',
    description = 'Iterative JSON parser with a standard Python iterator interface',
    long_description = open('README.rst').read(),

    classifiers = [
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    packages = find_packages(),
)

# Check if the yajl library + headers are present
# We don't use compiler.has_function because it leaves a lot of files behind
# without properly cleaning up
def yajl_present():

    compiler = distutils.ccompiler.new_compiler(verbose=1)
    distutils.sysconfig.customize_compiler(compiler) # CC, CFLAGS, LDFLAGS, etc

    fname = tempfile.mktemp(".c", "yajl_version")
    try:
        with open(fname, "wt") as f:
            f.write('#include <yajl/yajl_version.h>\nint main(int args, char **argv) { yajl_version(); return 0; }')

        try:
            objs = compiler.compile([fname])
            compiler.link_shared_lib(objs, 'a', libraries=["yajl"])
            return True
        finally:
            os.remove(compiler.library_filename('a', lib_type='shared'))
            for obj in objs:
                os.remove(obj)

    except:
        return False
    finally:
        if os.path.exists(fname):
            os.remove(fname)

# Conditional compilation of the yajl_c backend
if platform.python_implementation() == 'CPython':
    if yajl_present():
        yajl_ext = Extension('ijson.backends._yajl2',
                             language='c',
                             sources = ['ijson/backends/_yajl2.c'],
                             libraries = ['yajl'])
        setupArgs['ext_modules'] = [yajl_ext]

setup(**setupArgs)