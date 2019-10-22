import re

import aiomysql
import aiofiles

from fake_useragent import UserAgent
from jelly import Spider


MYSQL_OPTIONS = dict(
    host='127.0.0.1',
    db='test',
    user='root',
    password='root',
    charset="utf8"
)

class Mysql:
    @classmethod
    def __init__(self, mysql_pool):
        self._mysql_pool = mysql_pool

    async def execute(self, sql):
        async with self._mysql_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)
                conn.commit()
                return cur.rowcount, cur.lastrowid


class MySpider(Spider):
    concurrency = 10
    ua = UserAgent()
    headers = {'User-Agent': ua.random}
    start_url = 'http://quotes.toscrape.com/'
    proxy = 'xxx.xxx.xxx.xxx:xxxx'

    async def initialize(self):
        # Initialize the asynchronous mysql connection pool
        self.mysql = Mysql(await aiomysql.create_pool(**MYSQL_OPTIONS))

    async def parse(self, response):
        # Set the headers for the next request
        MySpider.headers = {'User-Agent': MySpider.ua.random}
        # Set the proxy for the next request
        MySpider.proxy = 'xxx.xxx.xxx.xxx:xxxx'
        urls = re.findall(r'<a href="(.*?)">\(about\)', response.html)
        # Put the url in the queue to be climbed
        self.put_url(urls, callback=self.qoute_parse)

    async def qoute_parse(self, response):
        title = re.search(r'<strong>Description:</strong>', response.html)
        await self.mysql.execute('INSERT ...')
        async with aiofiles.open('data.txt', mode='w') as f:
            f.write('{}\r\n'.format(title))


if __name__ == '__main__':
    MySpider.run()