.. image:: https://travis-ci.org/isagalaev/ijson.svg?branch=master
    :target: https://travis-ci.org/isagalaev/ijson

=====
ijson
=====

Ijson is an iterative JSON parser with a standard Python iterator interface.


Usage
=====

All usage example will be using a JSON document describing geographical
objects::

    {
      "earth": {
        "europe": [
          {"name": "Paris", "type": "city", "info": { ... }},
          {"name": "Thames", "type": "river", "info": { ... }},
          // ...
        ],
        "america": [
          {"name": "Texas", "type": "state", "info": { ... }},
          // ...
        ]
      }
    }

Most common usage is having ijson yield native Python objects out of a JSON
stream located under a prefix. Here's how to process all European cities::

    import ijson

    f = urlopen('http://.../')
    objects = ijson.items(f, 'earth.europe.item')
    cities = (o for o in objects if o['type'] == 'city')
    for city in cities:
        do_something_with(city)

Sometimes when dealing with a particularly large JSON payload it may worth to
not even construct individual Python objects and react on individual events
immediately producing some result::

    import ijson

    parser = ijson.parse(urlopen('http://.../'))
    stream.write('<geo>')
    for prefix, event, value in parser:
        if (prefix, event) == ('earth', 'map_key'):
            stream.write('<%s>' % value)
            continent = value
        elif prefix.endswith('.name'):
            stream.write('<object name="%s"/>' % value)
        elif (prefix, event) == ('earth.%s' % continent, 'end_map'):
            stream.write('</%s>' % continent)
    stream.write('</geo>')


For ``asyncio`` backend usage will be a bit different, depending on your Python
version. For Python 3.5 you can stay with the familiar API, thanks to
`async for` feature::

    import ijson.backends.asyncio as ijson

    async def go():
        resp = await aiohttp.get('http://.../')
        async for item in ijson.items(resp.content, 'earth.europe.item'):
            ...

Python 3.3 and 3.4 lacks that feature, so you'll have to go with the `while`
loop instead::

    import asyncio
    import ijson.backends.asyncio as ijson

    @asyncio.coroutine
    def go():
        resp = yield from aiohttp.get('http://.../')
        items = ijson.items(resp.content, 'earth.europe.item')
        while True:
            try:
                item = yield from items.next()
            except ijson.StopAsyncIteration:  # signal that steam is over
                break
            else:
                ...

Backends
========

Ijson provides several implementations of the actual parsing in the form of
backends located in ijson/backends:

- ``yajl2_c``: a C extension using `YAJL <http://lloyd.github.com/yajl/>`_ 2.x.
  This is the fastest, but requires a compiler and the YAJL development files
  to be present when installing this package.
- ``yajl2_cffi``: wrapper around `YAJL <http://lloyd.github.com/yajl/>`_ 2.x
  using CFFI.
- ``yajl2``: wrapper around YAJL 2.x using ctypes, for when you can't use CFFI
  for some reason.
- ``yajl``: deprecated YAJL 1.x + ctypes wrapper, for even older systems.
- ``python``: pure Python parser, good to use with PyPy
- ``asyncio``: pure Python parser made for asyncio

You can import a specific backend and use it in the same way as the top level
library::

    import ijson.backends.yajl2_cffi as ijson

    for item in ijson.items(...):
        # ...

With `asyncio` backend it's possible to reuse `yajl*` ones for parsing JSON
data. This will improve parsing performance with preserving async IO::

    import ijson.backends.asyncio as ijson
    from ijson.backends import yajl2

    async def go():
        resp = await aiohttp.get('http://.../')
        async for item in ijson.items(resp.content, 'earth.europe.item', yajl_backend=yajl2)
            ...

Importing the top level library as ``import ijson`` uses the pure Python
backend.


Acknowledgements
================

Python parser in ijson is relatively simple thanks to `Douglas Crockford
<http://www.crockford.com/>`_ who invented a strict, easy to parse syntax.

The `YAJL <http://lloyd.github.com/yajl/>`_ library by `Lloyd Hilaiel
<http://lloyd.io/>`_ is the most popular and efficient way to parse JSON in an
iterative fashion.

Ijson was inspired by `yajl-py <http://pykler.github.com/yajl-py/>`_ wrapper by
`Hatem Nassrat <http://www.nassrat.ca/>`_. Though ijson borrows almost nothing
from the actual yajl-py code it was used as an example of integration with yajl
using ctypes.
