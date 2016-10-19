from importlib import import_module

from setuptools import setup, find_packages, Extension
import platform
import distutils.ccompiler

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

# Conditional compilation of the yajl_c backend
if platform.python_implementation() == 'CPython':
    try:
        compiler = distutils.ccompiler.new_compiler()
        yajl_present = compiler.has_function('yajl_version', includes=["yajl/yajl_version.h"], libraries=['yajl'])
    except:
        yajl_present = False

    if yajl_present:
        yajl_ext = Extension('ijson.backends._yajl2',
                             language='c',
                             sources = ['ijson/backends/_yajl2.c'],
                             libraries = ['yajl'])
        setupArgs['ext_modules'] = [yajl_ext]

setup(**setupArgs)