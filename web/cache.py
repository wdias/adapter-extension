"""
Caching mechanism for Extension is different.
1. It try to optimize the cache for OnChange requests
  - In this case, it insert into the existing cache on Onchange Redis Hash while trying to create an extension
  - Thus it need to invalidate the cache separately
TODO: Just cache the extension_ids against the OnChange timeseries, and use a single Redis Schema to cache the extensions.
      Then retrieve extension data from that schema.
2. It send the new extension creation data to the extension-scheduler for realtime cron job creations using Redis List
  - Since extension-scheduler is suppose to be a singleton container, may be using Redis Streams add more complexity
"""
import os
import redis
import json
from typing import List


class Cache:
    def __init__(self):
        self.redis = redis.Redis(
            host=os.getenv('redisHost', 'adapter-redis-master.default.svc.cluster.local'),
            port=os.getenv('redisPort', 6379), db=os.getenv('redisDB', 1),
            password=os.getenv('redisPassword', 'wdias123'))

    def set(self, key: str, data: dict):
        return self.redis.set(key, json.dumps(data))

    def get(self, key: str):
        data = self.redis.get(key)
        if data is None:
            return data
        return json.loads(data)

    def mget(self, keys: List[str]):
        return self.redis.mget(keys)

    def delete(self, key: str):
        return self.redis.delete(key)

    def hset_pipe_on_change_timeseries_extension_by_ids(self, timeseries_ids: List[str], extensionId, extension, function, data, options, *args, **kargs):
        pipe = self.redis.pipeline()
        extension = {
                'extensionId': extensionId,
                'extension': extension,
                'function': function,
                'data': data,
                'options': options,
            }
        for ts in timeseries_ids:
            pipe.hset(ts, extensionId, json.dumps(extension))
        return pipe.execute()

    def hset_on_change_timeseries_extension(self, timeseries_id: str, extension: dict):
        return self.redis.hset(timeseries_id, extension.get('extensionId'), json.dumps(extension))

    def hmset_on_change_timeseries_extension(self, timeseries_id: str, extensions):
        return self.redis.hmset(timeseries_id, dict((extension.extensionId, extension) for extension in extensions))

    def hgetall_on_change_extensions_by_timeseries(self, timeseries_id: str) -> list:
        return list(json.loads(ex) for ex in self.redis.hgetall(timeseries_id).values())

    def hdel_pipe_on_change_extension(self, timeseries_ids: List[str], extension_ids: List[str]):
        pipe = self.redis.pipeline()
        for ts in timeseries_ids:
            pipe.hdel(ts, *extension_ids)
        return pipe.execute()


class TriggerScheduler:
    def __init__(self):
        self.scheduler_list = 'trigger_scheduler'
        self.redis = redis.Redis(
            host=os.getenv('redisHost', 'adapter-redis-master.default.svc.cluster.local'),
            port=os.getenv('redisPort', 6379), db=os.getenv('redisDB', 1),
            password=os.getenv('redisPassword', 'wdias123'))

    def add_to_scheduler(self, trigger_on, extensionId, extension, function, data, options, *args, **kargs):
        extensions = [{
            'extensionId': extensionId,
            'extension': extension,
            'function': function,
            'data': data,
            'options': options
        }]
        triggers = []
        for on in trigger_on:
            triggers.append({
                'trigger_on': on,
                'extensions': extensions
            })
        self.redis.lpush(self.scheduler_list, json.dumps(triggers))
