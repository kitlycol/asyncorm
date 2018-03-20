import asyncio
import os
import logging
from datetime import datetime
from decimal import Decimal as dec
from asyncorm.application.configure import configure_orm
from benchmarkasyncorm.simplemodel.models import Book

logger = logging.getLogger('benchmark')
logger.handler = logging.StreamHandler
logger.handler.formatter = '%(asctime)s %(name)s - %(levelname)s: %(message)s'

orm_app = configure_orm(os.path.join(os.getcwd(), 'asyncorm.ini'))
orm_app.sync_db()
loop = orm_app.loop


async def create_book(base, number):
    kwargs = {
        'name': 'book_{}'.format(number),
        'content': 'hard cover',
        'price': dec('145.5') + number,
    }
    await Book.objects.create(**kwargs)


for x, y in [('a', 300), ('b', 3000), ('c', 30000), ('d', 300000), ('e', 3000000)]:
    print('creating {} books'.format(y))
    start_time = datetime.now()
    tasks = []
    asyncio.gather(*[create_book(x, y) for y in range(1, )])
    for x in range(y):
        task = loop.create_task(create_book('CCC', x))
        tasks.append(asyncio.gather(task))
    loop.run_until_complete(asyncio.gather(*tasks))
    final_time = datetime.now() - start_time
    print('{} books created in {} seconds'.format(y, final_time.total_seconds()))
