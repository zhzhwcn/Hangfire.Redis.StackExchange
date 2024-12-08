// Copyright © 2013-2015 Sergey Odinokov, Marco Casamento 
// This software is based on https://github.com/HangfireIO/Hangfire.Redis 

// Hangfire.Redis.StackExchange is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire.Redis.StackExchange is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire.Redis.StackExchange. If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FreeRedis;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Redis.StackExchange
{
    internal class RedisWriteDirectlyToDatabase : JobStorageTransaction
    {
        private readonly RedisStorage _storage;
        private readonly RedisClient _redisClient;

        public RedisWriteDirectlyToDatabase([NotNull] RedisStorage storage, [NotNull] RedisClient redisClient)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _redisClient = redisClient ?? throw new ArgumentNullException(nameof(redisClient));
        }

        public override void AddRangeToSet([NotNull] string key, [NotNull] IList<string> items)
        {
            _redisClient.ZAdd(_storage.GetRedisKey(key), items.Select(x => new ZMember(x, 0)).ToArray());
        }

        public override void ExpireHash([NotNull] string key, TimeSpan expireIn)
        {
            _redisClient.Expire(_storage.GetRedisKey(key), expireIn);
        }

        public override void ExpireList([NotNull] string key, TimeSpan expireIn)
        {
            _redisClient.Expire(_storage.GetRedisKey(key), expireIn);
        }

        public override void ExpireSet([NotNull] string key, TimeSpan expireIn)
        {
            _redisClient.Expire(_storage.GetRedisKey(key), expireIn);
        }

        public override void PersistHash([NotNull] string key)
        {
            _redisClient.Persist(_storage.GetRedisKey(key));
        }

        public override void PersistList([NotNull] string key)
        {
            _redisClient.Persist(_storage.GetRedisKey(key));
        }

        public override void PersistSet([NotNull] string key)
        {
            _redisClient.Persist(_storage.GetRedisKey(key));
        }

        public override void RemoveSet([NotNull] string key)
        {
            _redisClient.Del(_storage.GetRedisKey(key));
        }

        public override void Commit()
        {
            //nothing to be done
        }

        public override void ExpireJob([NotNull] string jobId, TimeSpan expireIn)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            var tasks = new Task[3];

            tasks[0] = _redisClient.ExpireAsync(_storage.GetRedisKey($"job:{jobId}"), expireIn);
            tasks[1] = _redisClient.ExpireAsync(_storage.GetRedisKey($"job:{jobId}:history"), expireIn);
            tasks[2] = _redisClient.ExpireAsync(_storage.GetRedisKey($"job:{jobId}:state"), expireIn);

            Task.WaitAll(tasks);
        }

        public override void PersistJob([NotNull] string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            var tasks = new Task[3];

            tasks[0] = _redisClient.PersistAsync(_storage.GetRedisKey($"job:{jobId}"));
            tasks[1] = _redisClient.PersistAsync(_storage.GetRedisKey($"job:{jobId}:history"));
            tasks[2] = _redisClient.PersistAsync(_storage.GetRedisKey($"job:{jobId}:state"));

            Task.WaitAll(tasks);
        }

        public override void SetJobState([NotNull] string jobId, [NotNull] IState state)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));
            if (state == null) throw new ArgumentNullException(nameof(state));

            var tasks = new Task[3];

            tasks[0] = _redisClient.HSetAsync(_storage.GetRedisKey($"job:{jobId}"), "State", state.Name);
            tasks[1] = _redisClient.DelAsync(_storage.GetRedisKey($"job:{jobId}:state"));

            var storedData = new Dictionary<string, string>(state.SerializeData())
            {
                {"State", state.Name}
            };

            if (state.Reason != null)
                storedData.Add("Reason", state.Reason);

            tasks[2] = _redisClient.HMSetAsync(_storage.GetRedisKey($"job:{jobId}:state"), storedData);

            AddJobState(jobId, state);

            Task.WaitAll(tasks);
        }

        public override void AddJobState([NotNull] string jobId, [NotNull] IState state)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));
            if (state == null) throw new ArgumentNullException(nameof(state));

            var storedData = new Dictionary<string, string>(state.SerializeData())
            {
                {"State", state.Name},
                {"Reason", state.Reason},
                {"CreatedAt", JobHelper.SerializeDateTime(DateTime.UtcNow)}
            };

            _redisClient.RPush(
                _storage.GetRedisKey($"job:{jobId}:history"),
                SerializationHelper.Serialize(storedData));
        }

        public override void AddToQueue([NotNull] string queue, [NotNull] string jobId)
        {
            if (queue == null) throw new ArgumentNullException(nameof(queue));
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            var tasks = new Task[3];

            tasks[0] = _redisClient.SAddAsync(_storage.GetRedisKey("queues"), queue);
            if (_storage.LifoQueues != null && _storage.LifoQueues.Contains(queue, StringComparer.OrdinalIgnoreCase))
            {
                tasks[1] = _redisClient.RPushAsync(_storage.GetRedisKey($"queue:{queue}"), jobId);
            }
            else
            {
                tasks[1] = _redisClient.LPushAsync(_storage.GetRedisKey($"queue:{queue}"), jobId);
            }

            tasks[2] = _redisClient.PublishAsync(_storage.SubscriptionChannel, jobId);

            Task.WaitAll(tasks);
        }

        public override void IncrementCounter([NotNull] string key)
        {
            _redisClient.Incr(_storage.GetRedisKey(key));
        }

        public override void IncrementCounter([NotNull] string key, TimeSpan expireIn)
        {
            var tasks = new Task[2];
            
            tasks[0] = _redisClient.IncrAsync(_storage.GetRedisKey(key));
            tasks[1] = _redisClient.ExpireAsync(_storage.GetRedisKey(key), expireIn);

            Task.WaitAll(tasks);
        }

        public override void DecrementCounter([NotNull] string key)
        {
            _redisClient.Decr(_storage.GetRedisKey(key));
        }

        public override void DecrementCounter([NotNull] string key, TimeSpan expireIn)
        {
            var tasks = new Task[2];
            
            tasks[0] = _redisClient.DecrAsync(_storage.GetRedisKey(key));
            tasks[1] = _redisClient.ExpireAsync(_storage.GetRedisKey(key), expireIn);

            Task.WaitAll(tasks);
        }

        public override void AddToSet([NotNull] string key, [NotNull] string value)
        {
            AddToSet(key, value, 0);
        }

        public override void AddToSet([NotNull] string key, [NotNull] string value, double score)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));

            _redisClient.ZAdd(_storage.GetRedisKey(key), (decimal)score, value);
        }

        public override void RemoveFromSet([NotNull] string key, [NotNull] string value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));

            _redisClient.ZRem(_storage.GetRedisKey(key), value);
        }

        public override void InsertToList([NotNull] string key, string value)
        {
            _redisClient.LPush(_storage.GetRedisKey(key), value);
        }

        public override void RemoveFromList([NotNull] string key, string value)
        {
            _redisClient.LRem(_storage.GetRedisKey(key), 0, value);
        }

        public override void TrimList([NotNull] string key, int keepStartingFrom, int keepEndingAt)
        {
            _redisClient.LTrim(_storage.GetRedisKey(key), keepStartingFrom, keepEndingAt);
        }

        public override void SetRangeInHash(
            [NotNull] string key, [NotNull] IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            if (keyValuePairs is Dictionary<string, string> dic)
            {
                _redisClient.HMSet(_storage.GetRedisKey(key), dic);
            }
            else
            {
                _redisClient.HMSet(_storage.GetRedisKey(key), keyValuePairs.ToDictionary(x => x.Key, x => x.Value));
            }
        }

        public override void RemoveHash([NotNull] string key)
        {
            _redisClient.Del(_storage.GetRedisKey(key));
        }

        public override void Dispose()
        {
            //Don't have to dispose anything
        }
    }
}