'''
Pure-python parsing backend designed to work with asyncio coroutines.
'''

import asyncio
import builtins
import decimal

from ijson.backends import python
from ijson import common


try:
    StopAsyncIteration = builtins.StopAsyncIteration
except AttributeError:
    # Python 3.3/3.4 case
    class StopAsyncIteration(Exception):
        pass


class AsyncIterable(object):

    def __init__(self, coro):
        self.coro = coro

    @asyncio.coroutine
    def __aiter__(self):
        return self

    @asyncio.coroutine
    def __anext__(self):
        return (yield from self.next())

    @asyncio.coroutine
    def next(self):
        raise NotImplementedError


class FileReader(AsyncIterable):

    def __init__(self, coro, buf_size=python.BUFSIZE, encoding='utf-8'):
        super().__init__(coro)
        self.buf_size = buf_size
        self.encoding = encoding

    @asyncio.coroutine
    def next(self):
        return (yield from self.coro.read(self.buf_size)).decode(self.encoding)


class Lexer(AsyncIterable):

    def __init__(self, coro):
        super().__init__(coro)
        self.discarded = 0
        self.pos = 0
        self.buf = ''

    @asyncio.coroutine
    def next(self):
        if not self.buf:
            self.buf = yield from self.coro.next()
        if not self.buf:
            raise StopAsyncIteration
        while True:
            match = python.LEXEME_RE.search(self.buf, self.pos)
            if match:
                lexeme = match.group()
                if lexeme == '"':
                    self.pos = match.start()
                    start = self.pos + 1
                    while True:
                        end = self.buf.find('"', start)
                        if end == -1:
                            data = yield from self.coro.next()
                            if not data:
                                raise common.IncompleteJSONError('Incomplete string lexeme')
                            self.buf += data
                        else:
                            escpos = end - 1
                            while self.buf[escpos] == '\\':
                                escpos -= 1
                            if (end - escpos) % 2 == 0:
                                start = end + 1
                            else:
                                break
                    rval = self.discarded + self.pos, self.buf[self.pos:end + 1]
                    self.pos = end + 1
                    return rval
                else:
                    while match.end() == len(self.buf):
                        data = yield from self.coro.next()
                        if not data:
                            break
                        self.buf += data
                        match = python.LEXEME_RE.search(self.buf, self.pos)
                        lexeme = match.group()
                    self.pos = match.end()
                    return self.discarded + match.start(), lexeme
            else:
                data = yield from self.coro.next()
                if not data:
                    raise StopAsyncIteration
                self.discarded += len(self.buf)
                self.buf = data
                self.pos = 0


@asyncio.coroutine
def parse_value(lexer, pos=0, symbol=None):
    if symbol is None:
        pos, symbol = yield from lexer.next()
    if symbol == 'null':
        return ('null', None)
    elif symbol == 'true':
        return ('boolean', True)
    elif symbol == 'false':
        return ('boolean', False)
    elif symbol == '[':
        return ArrayParser(lexer)
    elif symbol == '{':
        return ObjectParser(lexer)
    elif symbol[0] == '"':
        return ('string', python.unescape(symbol[1:-1]))
    else:
        try:
            return ('number', common.number(symbol))
        except decimal.InvalidOperation:
            raise python.UnexpectedSymbol(symbol, pos)


class BasicParser(AsyncIterable):

    def __init__(self, lexer):
        super().__init__(lexer)
        self._parser = None
        self._completed = False

    @asyncio.coroutine
    def next(self):
        if self._completed:
            return None
        if self._parser is None:
            value = yield from parse_value(self.coro)
            if isinstance(value, ContainerParser):
                self._parser = value
            else:
                self._completed = True
                return value
        value = yield from self._parser.next()
        if value is None:
            self._completed = True
        return value


class ContainerParser(AsyncIterable):

    @asyncio.coroutine
    def next_symbol(self):
        try:
            pos, symbol = yield from self.coro.next()
        except StopAsyncIteration:
            raise common.IncompleteJSONError('Incomplete JSON data')
        else:
            return pos, symbol


class ArrayParser(ContainerParser):

    state = None
    _parser = None

    @asyncio.coroutine
    def next(self):
        if self.state is None:
            self.state = 'new_item'
            return ('start_array', None)
        elif self.state == 'ended':
            return
        elif self.state == 'new_item':
            return (yield from self.next_new_item())
        elif self.state == 'next_item':
            return (yield from self.next_item())
        else:
            raise RuntimeError(self.state)

    @asyncio.coroutine
    def next_new_item(self):
        pos, symbol = yield from self.next_symbol()
        if symbol == ']':
            self.state = 'ended'
            return ('end_array', None)
        value = yield from parse_value(self.coro, pos, symbol)
        self.state = 'next_item'
        if isinstance(value, tuple):
            return value
        self._parser = value
        return (yield from self._parser.next())

    @asyncio.coroutine
    def next_item(self):
        if self._parser is not None:
            item = yield from self._parser.next()
            if item is not None:
                return item
            self._parser = None
            pos, symbol = yield from self.next_symbol()
            if symbol == ']':
                self.state = 'ended'
                return ('end_array', None)
            if symbol != ',':
                raise python.UnexpectedSymbol(symbol, pos)
        value = yield from parse_value(self.coro)
        self.state = 'next_item'
        if isinstance(value, tuple):
            return value
        self._parser = value
        return (yield from self._parser.next())


class ObjectParser(ContainerParser):

    state = None
    _parser = None

    @asyncio.coroutine
    def next(self):
        if self.state is None:
            self.state = 'first_key'
            return ('start_map', None)
        elif self.state == 'ended':
            return
        elif self.state == 'first_key':
            rval = self.next_first_key()
        elif self.state == 'key':
            rval = self.next_key()
        elif self.state == 'before_value':
            rval = self.before_value()
        elif self.state == 'value':
            rval = self.next_value()
        elif self.state == 'after_value':
            rval = self.after_value()
        else:
            raise RuntimeError('invalid state %r' % self.state)
        return (yield from rval)

    @asyncio.coroutine
    def next_first_key(self):
        pos, symbol = yield from self.next_symbol()
        if symbol == '}':
            self.state = 'ended'
            return ('end_map', None)
        return (yield from self.next_key(pos, symbol))

    @asyncio.coroutine
    def next_key(self, pos=0, symbol=None):
        if symbol is None:
            pos, symbol = yield from self.next_symbol()
        if symbol[0] != '"':
            raise python.UnexpectedSymbol(symbol, pos)
        self.state = 'before_value'
        return ('map_key', python.unescape(symbol[1:-1]))

    @asyncio.coroutine
    def before_value(self):
        pos, symbol = yield from self.next_symbol()
        if symbol[0] != ':':
            raise python.UnexpectedSymbol(symbol, pos)
        return (yield from self.next_value())

    @asyncio.coroutine
    def next_value(self):
        self.state = 'value'
        if self._parser is not None:
            item = yield from self._parser.next()
            if item is not None:
                return item
            self._parser = None
            return (yield from self.after_value())
        else:
            value = yield from parse_value(self.coro)
            if isinstance(value, tuple):
                self.state = 'after_value'
                return value
            self._parser = value
            return (yield from self._parser.next())

    @asyncio.coroutine
    def after_value(self):
        pos, symbol = yield from self.next_symbol()
        if symbol == '}':
            self.state = 'ended'
            return ('end_map', None)
        if symbol != ',':
            raise python.UnexpectedSymbol(symbol, pos)
        return (yield from self.next_key())


class Parser(AsyncIterable):

    def __init__(self, coro):
        super().__init__(coro)
        self.path = []

    @asyncio.coroutine
    def next(self):
        path = self.path
        item = yield from self.coro.next()
        if item is None:
            raise StopAsyncIteration
        event, value = item
        if event == 'map_key':
            prefix = '.'.join(path[:-1])
            path[-1] = value
        elif event == 'start_map':
            prefix = '.'.join(path)
            path.append(None)
        elif event == 'end_map':
            path.pop()
            prefix = '.'.join(path)
        elif event == 'start_array':
            prefix = '.'.join(path)
            path.append('item')
        elif event == 'end_array':
            path.pop()
            prefix = '.'.join(path)
        else:  # any scalar value
            prefix = '.'.join(path)
        return prefix, event, value


class Items(AsyncIterable):

    def __init__(self, coro, prefix):
        super().__init__(coro)
        self.prefix = prefix

    @asyncio.coroutine
    def next(self):
        while True:
            current, event, value = yield from self.coro.next()
            if current != self.prefix:
                continue
            if event in ('start_map', 'start_array'):
                builder = common.ObjectBuilder()
                end_event = event.replace('start', 'end')
                while (current, event) != (self.prefix, end_event):
                    builder.event(event, value)
                    current, event, value = yield from self.coro.next()
                return builder.value
            else:
                return value


def basic_parse(file, buf_size=python.BUFSIZE):
    return BasicParser(Lexer(FileReader(file, buf_size)))


def parse(file, buf_size=python.BUFSIZE):
    '''
    Backend-specific wrapper for ijson.common.parse.
    '''
    return Parser(basic_parse(file, buf_size))


def items(file, prefix):
    '''
    Backend-specific wrapper for ijson.common.items.
    '''
    return Items(parse(file), prefix)
