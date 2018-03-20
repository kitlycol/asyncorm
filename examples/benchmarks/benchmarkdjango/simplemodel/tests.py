import asyncio
import os
import unittest
from asyncorm.application.configure import configure_orm
from decimal import decimal as dec


class AioTestCase(unittest.TestCase):

    # noinspection PyPep8Naming
    def __init__(self, methodName='runTest', loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self._function_cache = {}
        super(AioTestCase, self).__init__(methodName=methodName)

    def coroutine_function_decorator(self, func):
        def wrapper(*args, **kw):
            return self.loop.run_until_complete(func(*args, **kw))
        return wrapper

    def __getattribute__(self, item):
        attr = object.__getattribute__(self, item)
        if asyncio.iscoroutinefunction(attr):
            if item not in self._function_cache:
                self._function_cache[item] = self.coroutine_function_decorator(attr)
            return self._function_cache[item]
        return attr


class BenchmarkTest(AioTestCase):

    async def create_book(book):
        from simplemodel.models import Book

        for x in range(100):
            kwargs = {
                'name': 'book_{}'.format(x),
                'content': 'this is a simple content {}'.format(x),
                'price': dec('145.5') + x,
                'quantity': x,
            }
            await Book.objects.create(**kwargs)


config_file = os.path.join(os.getcwd(), 'tests', 'asyncorm.ini')
orm_app = configure_orm(config_file)

loop = orm_app.loop

unittest.main()
