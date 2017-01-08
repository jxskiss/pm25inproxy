# -*- coding: utf-8 -*-
#
# Copyright (c) 2016. All rights reserved.
#
# @author: jxs <jxskiss@126.com>
# @created: 16-8-5
#

"""PM25.in data crawler and router.

The API provided by pm25.in has a request count limit rather small, this
script download the data from pm25.in and serves requests within company
as a router service. The inside air data crawlers should all request from
this service instead of pm25.in.

This router service should run on a server with large bandwidth to avoid
failed requests, which consume the request limits without results.

Version history:

2016-08-05: initial version, features:
    1. application cache support
    2. update data from pm25.in periodically
    3. saving data to local disk
    4. archive old data by month periodically
    5. simple history data query support

2016-09-26: improvements:
    1. Save history data to disk only when the data is updated to
       decrease disk space occupation.
    2. Save history data files gzipped to decrease space occupation
       Uncompressed history data file is supported for reading to keep
       compatible with old history data files.
    3. Remove old history data files periodically.
"""

import os
import sys
import json
import time
import datetime
import re
import sqlite3
import gzip
from functools import partial
from collections import namedtuple, defaultdict, Counter
from operator import itemgetter
from tornado import web, gen, ioloop
from tornado.httpclient import AsyncHTTPClient, HTTPError
from tornado.httputil import url_concat
from tornado.log import gen_log
from tornado.options import options, parse_command_line

if sys.version_info[0] >= 3:
    basestring = (str, bytes)

# pm25.in API definition, see pm25.in documentation for details:
# http://pm25.in/api_doc
PM25IN_API_TUPLE = namedtuple('PM25IN_API_TUPLE', ['id', 'limit'])
PM25IN_APIS = {
    # 1.1、获取一个城市所有监测点的PM2.5数据，限500次/小时
    'api/querys/pm2_5.json': PM25IN_API_TUPLE(1, 400),
    # 1.2、获取一个城市所有监测点的PM10数据，限500次/小时
    'api/querys/pm10.json': PM25IN_API_TUPLE(2, 400),
    # 1.3、获取一个城市所有监测点的CO数据，限500次/小时
    'api/querys/co.json': PM25IN_API_TUPLE(3, 400),
    # 1.4、获取一个城市所有监测点的NO2数据，限500次/小时
    'api/querys/no2.json': PM25IN_API_TUPLE(4, 400),
    # 1.5、获取一个城市所有监测点的SO2数据，限500次/小时
    'api/querys/so2.json': PM25IN_API_TUPLE(5, 400),
    # 1.6、获取一个城市所有监测点的O3数据，限500次/小时
    'api/querys/o3.json': PM25IN_API_TUPLE(6, 400),
    # 1.7、获取一个城市所有监测点的AQI数据（含详情），限500次/小时
    'api/querys/aqi_details.json': PM25IN_API_TUPLE(7, 400),
    # 1.8、获取一个城市所有监测点的AQI数据（不含详情，仅AQI），限500次/小时
    'api/querys/only_aqi.json': PM25IN_API_TUPLE(8, 400),
    # 1.9、获取一个监测点的AQI数据（含详情），限500次/小时
    'api/querys/aqis_by_station.json': PM25IN_API_TUPLE(9, 400),
    # 1.10、获取一个城市的监测点列表，限15次/小时
    'api/querys/station_names.json': PM25IN_API_TUPLE(10, 1),
    # 1.11、获取实施了新《环境空气质量标准》的城市列表，即有PM2.5数据的城市列表，限15次小时
    'api/querys.json': PM25IN_API_TUPLE(11, 1),
    # 1.12、获取所有城市的空气质量详细数据，限5次/小时
    'api/querys/all_cities.json': PM25IN_API_TUPLE(12, 3),
    # 1.13、获取全部城市的空气质量指数(AQI)排行榜，限15次/小时
    'api/querys/aqi_ranking.json': PM25IN_API_TUPLE(13, 3)
}


class Config(dict):
    """A dict that allows for object-like property access syntax."""
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, key, value):
        self[key] = value

config = Config(
    # pm25.in base api url
    pm25in_base_url='http://www.pm25.in/',
    # pm25.in app key
    pm25in_app_key='',
    # base data store directory
    base_store_dir='',
    # remove old history data files after kept_days
    history_kept_days=365,
)
options.define("app_key", type=str, help='app key for pm25.in service',
               callback=lambda key: config.update({'pm25in_app_key': key}))
options.define("store_dir", type=str, help='data store directory',
               callback=lambda path: config.update({'base_store_dir': path}))
options.define("kept_days", type=int,
               help='kept days for history data files before removed',
               callback=lambda kept: config.update({'history_kept_days': kept}))


class BrokerHandler(web.RequestHandler):
    # global data cache, all requests share the cache
    # NOTE: the 'last' field may be a datetime string in ISO format or a
    # collections.Counter object
    data_cache = defaultdict(lambda: {
        'time': 0, 'data': None, 'last': None, 'lock': False})
    # only the following apis are enabled, which accept no arguments, return
    # all data for cities or stations
    enabled_apis = {
        'api/querys/station_names.json',  # api 1.10
        'api/querys.json',                # api 1.11
        'api/querys/all_cities.json',     # api 1.12
        'api/querys/aqi_ranking.json'     # api 1.13
    }
    # scheduled apis to update periodically
    # NOTE: these apis' data all use the following format, and methods rely
    # on this format: [{..., 'time_point': 'Z'}, {...}, ...]
    sched_apis = ('api/querys/all_cities.json',
                  'api/querys/aqi_ranking.json')
    # history index, providing simple history data query
    history_index = None

    @gen.coroutine
    def get(self, api):
        if api not in self.enabled_apis:
            self.finish({'error': 'Unsupported API for broker service'})
            return
        history = self.get_argument('history', None)
        if history is not None:
            if len(history) != 10:
                self.finish({'error': 'Error history argument'})
            else:
                self.finish(self.get_history(api, history))
            return
        now = time.time()
        expire = 3600 / PM25IN_APIS[api].limit
        if now - self.data_cache[api]['time'] > expire:
            if not self.data_cache[api]['lock']:
                yield self.refresh_cache(api)
        self.finish(self.data_cache[api]['data'])

    @staticmethod
    def full_api_path(api_base):
        if api_base == 'querys.json':
            return 'api/' + api_base
        else:
            return 'api/querys/' + api_base

    @classmethod
    def get_api_last_time(cls, api):
        if api not in cls.enabled_apis:
            return "unsupported"
        if api not in cls.data_cache:
            return "uncached"
        # the last field can be a string or a collections.Counter object,
        # see the definition of BrokerHandler.data_cache for more details
        last = cls.data_cache[api]['last']
        if isinstance(last, basestring):
            return last
        return last.most_common(1)[0][0][:19]

    @classmethod
    def get_history(cls, api, history):
        """Simple history data query support

        :param api: the api full path to query
        :param history: history timestamp: "%Y%m%d%H"
        """
        api_base = api.split('/')[-1]
        cursor = cls.history_index.cursor()
        cursor.execute(
            "select filename from history where api=? and ymdh=?;",
            (api_base, history))
        files = [r[0] for r in cursor]
        cls.history_index.commit()
        if not files:
            return {}
        results = {}
        for fn in files:
            ts = re.split('[?@]', fn)[-1].replace('.gz', '')
            fn_full = os.path.join(config.base_store_dir, fn)
            fd = (gzip.open if fn.endswith('.gz') else open)(fn_full)
            results[ts] = json.load(fd, encoding='utf8')
            fd.close()
        return results

    # classmethod or staticmethod must be the most outer decorator
    @classmethod
    @gen.coroutine
    def refresh_cache(cls, api):
        if cls.data_cache[api]['lock']:
            return
        url = url_concat(config.pm25in_base_url + api,
                         {'token': config.pm25in_app_key})
        client = AsyncHTTPClient()
        cls.data_cache[api]['lock'] = True
        try:
            # the pm25.in service sometimes goes unstable, resulting some quite
            # long-time connections, give it a larger timeout
            _start = time.time()
            try:
                resp = yield client.fetch(
                    url, connect_timeout=60, request_timeout=600)
            except HTTPError as err:
                gen_log.error("%s: %s" % (err, api))
                # try again after a while
                yield gen.sleep(10)
                resp = yield client.fetch(
                    url, connect_timeout=60, request_timeout=600)
            data = json.loads(resp.body)
            if isinstance(data, dict) and 'error' in data:
                raise ValueError(data['error'])

            # check data update time
            now = datetime.datetime.now().replace(microsecond=0)
            is_data_updated = True
            if api in cls.sched_apis:
                # check the updated time for different stations/cities
                data_last = Counter(map(itemgetter('time_point'), data))
                for tm, cnt in data_last.most_common(2):
                    gen_log.info("%s time counts: %s %s/%s"
                                 % (api, tm, cnt, len(data)))
                if data_last == cls.data_cache[api]['last']:
                    is_data_updated = False
            else:
                data_last = now.isoformat()
            # refresh cache and save to disk/database only if data updated
            if not is_data_updated:
                return
            cls.data_cache[api]['data'] = resp.body
            # save the download data to disk and update history index
            api_base = api.split('/')[-1]
            fn = '{}@{}.gz'.format(api_base, now.strftime("%Y%m%d%H%M%S"))
            with gzip.open(os.path.join(
                    config.base_store_dir, 'latest', fn), 'wb') as fd:
                fd.write(resp.body)
            cursor = cls.history_index.cursor()
            cursor.execute(
                "insert or replace into history (api, ymdh, filename) "
                "values (?, ?, ?);", (api_base, now.strftime("%Y%m%d%H"),
                                      os.path.join('latest', fn)))
            cls.history_index.commit()
            # update last update time for the api
            cls.data_cache[api]['last'] = data_last
            _end = time.time()
            gen_log.info("%s updated, using %s seconds" % (api, _end - _start))
        except Exception as err:
            gen_log.exception("error occurred: %s" % err)
        finally:
            cls.data_cache[api].update({'time': time.time(), 'lock': False})

    @classmethod
    def load_history_index(cls):
        index_fn = os.path.join(config.base_store_dir, 'history.sqlite3')
        if not os.path.exists(index_fn):
            cls.history_index = sqlite3.connect(index_fn)
            cursor = cls.history_index.cursor()
            cursor.execute(
                "create table history (api text, ymdh text, filename text, "
                "primary key (api, filename));")
            cls.history_index.commit()
        else:
            cls.history_index = sqlite3.connect(index_fn)
            cursor = cls.history_index.cursor()

            # load the last data to cache
            cursor.execute("select api, max(ymdh || '?' || filename) as last "
                           "from history group by api;")
            for api, max in cursor:
                api = cls.full_api_path(api)
                if api not in cls.enabled_apis:
                    continue
                last_time = time.mktime(datetime.datetime.strptime(
                        re.split('[?@]', max)[-1].replace('.gz', ''),
                        "%Y%m%d%H%M%S").timetuple())
                max_fn = os.path.join(config.base_store_dir, max.split('?', 1)[-1])
                fd = (gzip.open if max_fn.endswith('.gz') else open)(max_fn)
                cls.data_cache[api] = {
                    'time': last_time, 'data': fd.read(), 'lock': False}
                fd.close()
                # get last update time for the cached api data
                if api in cls.sched_apis:
                    data_last = Counter(map(
                        itemgetter('time_point'),
                        json.loads(cls.data_cache[api]['data'])))
                else:
                    data_last = datetime.datetime.fromtimestamp(
                                                    last_time).isoformat()
                cls.data_cache[api]['last'] = data_last

    @classmethod
    def archive_old_data(cls):
        # shortcuts
        join, exists = os.path.join, os.path.exists
        base_dir = config.base_store_dir

        # judge datetime from filename
        today = datetime.datetime.today()
        date_pattern = re.compile(r"(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})")
        count = 0
        for fn in os.listdir(join(base_dir, 'latest')):
            match = date_pattern.search(fn)
            if not match:
                continue
            date = datetime.datetime(*map(int, match.groups()))
            if (today - date).days <= 30:
                continue
            month = date.strftime('%Y%m')
            if not exists(join(base_dir, month)):
                os.mkdir(join(base_dir, month))
            os.rename(join(base_dir, 'latest', fn), join(base_dir, month, fn))
            count += 1
            # update history index
            cursor = cls.history_index.cursor()
            cursor.execute(
                "update history set filename=? where api=? and filename=?;",
                (join(month, fn), re.split('[?@]', fn)[0], join('latest', fn)))
            cls.history_index.commit()
        if count > 0:
            gen_log.info("archived %s old files" % count)

    @classmethod
    def remove_expired_files(cls):
        expired = (datetime.datetime.today() - datetime.timedelta(
                days=config.history_kept_days)).strftime("%Y%m%d%H%M%S")
        cursor = cls.history_index.cursor()
        cursor.execute("select api, ymdh, filename from history "
                       "where ymdh < ?;", (expired, ))
        old_records = cursor.fetchall()
        cls.history_index.commit()
        for rec in old_records:
            try:
                os.remove(os.path.join(config.base_store_dir, rec[2]))
            except Exception as err:
                gen_log.error("cannot remove file: %s: %s" % (rec[2], err))
            else:
                cursor.execute(
                    "delete from history where api = ? and filename = ?;",
                    (rec[0], rec[2]))
                cls.history_index.commit()
                gen_log.info("old file %s removed" % rec[2])
            # try to remove empty directories
            # OSError will be raised if the directory is not empty
            try:
                os.rmdir(os.path.join(config.base_store_dir, rec[1][:6]))
            except Exception as err:
                pass

    @classmethod
    def prepare_schedtasks(cls):
        now = time.time()
        for idx, api in enumerate(cls.sched_apis):
            first_run = max(10 + idx * 10, (3600 / PM25IN_APIS[api].limit) - (
                now - cls.data_cache[api]['time']))

            # NOTE: default value for argument api must be used to make the
            # function a closure, or the function gets un-intended argument
            @gen.coroutine
            def scheduled(the_api=api):
                yield cls.refresh_cache(the_api)
                ioloop.PeriodicCallback(
                    partial(cls.refresh_cache, the_api),
                    3600 / PM25IN_APIS[the_api].limit * 1000).start()
            # register scheduled refresh tasks
            ioloop.IOLoop.current().call_later(first_run, scheduled)

        # archive old data periodically
        ioloop.PeriodicCallback(cls.archive_old_data, 24 * 3600 * 1000).start()

        # remove expired history data files periodically
        ioloop.PeriodicCallback(
            cls.remove_expired_files, 24 * 3600 * 1000).start()


class ProxyHandler(web.RequestHandler):
    # global api called counts, all requests share the cache
    # format: {appkey: {api: {'time': 0, 'count': 0}, }, }
    api_counts = defaultdict(
        lambda: defaultdict(lambda: {'time': 0, 'count': 0}))

    # the following apis are enabled for proxy
    enabled_apis = {
        'api/querys/pm2_5.json',            # api 1.1
        'api/querys/pm10.json',             # api 1.2
        'api/querys/co.json',               # api 1.3
        'api/querys/no2.json',              # api 1.4
        'api/querys/so2.json',              # api 1.5
        'api/querys/o3.json',               # api 1.6
        'api/querys/aqi_details.json',      # api 1.7
        'api/querys/only_aqi.json',         # api 1.8
        'api/querys/aqis_by_station.json'   # api 1.9
    }

    @gen.coroutine
    def get(self, api):
        if api not in self.enabled_apis:
            self.finish({'error': 'Unsupported API for proxy service'})
            return
        # use provided appkey or the default appkey
        appkey = self.get_argument('token', None)
        query = self.request.query or ''
        if appkey is None:
            # append token to query arguments
            appkey = config.pm25in_app_key
            query = query + ('&' if len(query) else '') + 'token=' + appkey
        elif appkey == '':
            # replace empty token argument
            appkey = config.pm25in_app_key
            token = re.compile(r"(&?)(\btoken\b=?)")
            query = token.sub(r'\1' + 'token=' + appkey, query)
        now = time.time()
        limit = 3600 / PM25IN_APIS[api].limit
        if now - self.api_counts[appkey][api]['time'] < limit:
            self.finish({'error': 'API called too frequently'})
            return
        # proxy the request to pm25.in
        try:
            resp = yield self.fetch(appkey, api, query)
        except HTTPError as err:
            self.set_status(err.code, err.message)
            self.finish(err.response.body)
        except Exception as err:
            self.set_status(500)
            self.finish({'error': 'Error proxy to pm25.in'})
            gen_log.exception("error occurred: %s" % err)
        else:
            self.finish(resp.body)

    @classmethod
    @gen.coroutine
    def fetch(cls, appkey, api, query, *args, **kwargs):
        request = config.pm25in_base_url + api + '?' + query
        client = AsyncHTTPClient()
        resp = yield client.fetch(request, *args, **kwargs)
        cls.api_counts[appkey][api]['time'] = time.time()
        cls.api_counts[appkey][api]['count'] += 1
        raise gen.Return(resp)


class StatusHandler(web.RequestHandler):
    """Service or api last updated time status."""

    def get(self, api):
        if not len(api):
            self.finish("ok")
            return
        self.finish(BrokerHandler.get_api_last_time(api))


def main():
    options.define('port', default=5059, type=int, help='listening port')
    parse_command_line()

    # check the required options
    if config.pm25in_app_key == "":
        raise SystemExit("the app_key must be specified")
    if config.base_store_dir == "":
        cwd = os.getcwd()
        gen_log.warn("the data directory is set to: %s" % cwd)
        config.base_store_dir = cwd

    # prepare directories
    if not os.path.exists(config.base_store_dir):
        os.mkdir(config.base_store_dir)
    latest_data_dir = os.path.join(config.base_store_dir, 'latest')
    if not os.path.exists(latest_data_dir):
        os.mkdir(latest_data_dir)

    handlers = [
        (r'/(api/querys/(?:pm2_5|pm10|co|no2|so2|o3)\.json)', ProxyHandler),
        (r'/(api/querys/(?:aqi_details|only_aqi|aqis_by_station)\.json)', ProxyHandler),
        (r'/(api/querys\.json)', BrokerHandler),
        (r'/(api/querys/(?:station_names|all_cities|aqi_ranking)\.json)', BrokerHandler),
        (r'/status/(.*)', StatusHandler)
    ]
    settings = {'debug': True}
    application = web.Application(handlers, **settings)
    application.listen(options.port)
    # load history index must be executed before prepare scheduled tasks
    BrokerHandler.load_history_index()
    BrokerHandler.prepare_schedtasks()
    ioloop.IOLoop.instance().start()

if __name__ == '__main__':
    main()
