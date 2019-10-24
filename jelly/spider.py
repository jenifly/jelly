import asyncio
import re
import time

from datetime import datetime
from collections import deque
from asyncio import Queue
from urllib.parse import urljoin
from html import unescape

import aiohttp

from .log import logger

try:
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass


class Spider:
    start_url = ''
    base_url = None
    parsing_urls = deque()
    pre_parse_urls = Queue()
    filter_urls = set()
    item = None
    done_urls = []
    error_urls = []
    urls_count = 0
    concurrency = 5
    interval = None  # Limit the interval between two requests
    headers = {}
    proxy = None
    cookie_jar = None

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def is_running(cls):
        a = not cls.pre_parse_urls.empty() or len(cls.parsing_urls)
        return a

    @classmethod
    def put_url(cls, urls, callback=None):
        if not isinstance(urls, list):
            return cls._put_url(urls, callback)
        for url in urls:
            cls._put_url(url, callback)

    @classmethod
    def _put_url(cls, url, callback):
        url = unescape(url)
        if not re.match(r'(http|https)://', url):
            url = urljoin(cls.base_url, url)
        if url not in cls.filter_urls:
            cls.filter_urls.add(url)
            if callback:
                cls.pre_parse_urls.put_nowait((url, callback))
            else:
                cls.pre_parse_urls.put_nowait(url)

    @classmethod
    def run(cls):
        if not isinstance(cls.start_url, list):
            cls.start_url = [cls.start_url]
        if cls.base_url is None:
            cls.base_url = re.match(
                r'(http|https)://[\w\-_]+(\.[\w\-_]+)+/', cls.start_url[0]).group()
            logger.info('Base url: {}'.format(cls.base_url))
        for url in cls.start_url:
            cls.put_url(url)
        logger.info('Spider started!')
        start_time = datetime.now()
        loop = asyncio.get_event_loop()
        try:
            semaphore = asyncio.Semaphore(cls.concurrency)
            loop.run_until_complete(asyncio.gather(cls().initialize(), cls._task(semaphore)))
        except KeyboardInterrupt:
            for task in asyncio.Task.all_tasks():
                task.cancel()
            loop.run_forever()
        finally:
            end_time = datetime.now()
            logger.info('Requests count: {}'.format(cls.urls_count))
            logger.info('Error count: {}'.format(len(cls.error_urls)))
            logger.info('Time usage: {}'.format(end_time - start_time))
            logger.info('Spider finished!')
            loop.close()

    async def initialize(self):
        pass

    @classmethod
    async def _task(cls, semaphore):
        async with aiohttp.ClientSession(cookie_jar=cls.cookie_jar) as session:
            while cls.is_running():
                try:
                    url = await asyncio.wait_for(cls.pre_parse_urls.get(), 5)
                    cls.parsing_urls.append(url)
                    asyncio.ensure_future(
                        cls._execute_url(url, session, semaphore))
                except asyncio.TimeoutError:
                    pass

    @classmethod
    async def _execute_url(cls, url, session, semaphore):
        callback = url[1] if isinstance(url, tuple) else None
        response = await cls._fetch(url[0] if callback else url, session, semaphore)

        if response is None:
            cls.error_urls.append(url)
            cls.pre_parse_urls.put_nowait(url)
            # failure to retry or change your proxy
            return None

        if url in cls.error_urls:
            cls.error_urls.remove(url)
        cls.urls_count += 1
        cls.parsing_urls.remove(url)
        cls.done_urls.append(url)

        if callback:
            await callback(response)
            url = url[0]
        else:
            await cls().parse(response)

        logger.info('Parsed({}/{}): {}'.format(len(cls.done_urls),
                                               len(cls.filter_urls), url))

    async def parse(self, html):
        raise NotImplementedError

    @classmethod
    async def _fetch(cls, url, session, semaphore):
        if cls.interval:
            time.sleep(cls.interval)
        async with semaphore:
            try:
                async with session.get(url, headers=cls.headers, proxy=cls.proxy) as response:
                    if response.status in [200, 201]:
                        return Response({'html': await response.text(), 'url': response.url, 'headers': response.headers, 'cookies': response.cookies})
                    logger.error('Error: {} {}'.format(url, response.status))
                    return None
            except:
                return None


class Response:
    def __init__(self, attr):
        self.attr = attr

    def __getattr__(self, item):
        return self.attr[item]
