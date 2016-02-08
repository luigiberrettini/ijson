import inspect
import unittest


def discover_tests(mod):
    for key, item in inspect.getmembers(mod):
        if inspect.isclass(item) and issubclass(item, unittest.TestCase):
            yield unittest.makeSuite(item, 'test')


def common_tests():
    from . import test_common
    suite = unittest.TestSuite()
    for test in discover_tests(test_common):
        suite.addTest(test)
    return suite


def asyncio_tests():
    from . import test_asyncio
    suite = unittest.TestSuite()
    for test in discover_tests(test_asyncio):
        suite.addTest(test)
    return suite


def suite():
    suite = unittest.TestSuite()
    suite.addTest(common_tests())
    try:
        suite.addTest(asyncio_tests())
    except:
        pass
    return suite


unittest.main(defaultTest='suite')
