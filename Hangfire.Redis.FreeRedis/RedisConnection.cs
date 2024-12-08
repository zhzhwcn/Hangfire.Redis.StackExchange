// Copyright � 2013-2015 Sergey Odinokov, Marco Casamento
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

using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Storage;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FreeRedis;

namespace Hangfire.Redis.StackExchange
{
    internal class RedisConnection : JobStorageConnection
    {
        private readonly RedisStorage _storage;
        private readonly RedisSubscription _subscription;
        private readonly TimeSpan _fetchTimeout;

        public RedisConnection(
            [NotNull] RedisStorage storage,
            [NotNull] RedisClient redis,
            [NotNull] RedisSubscription subscription,
            TimeSpan fetchTimeout)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _subscription = subscription ?? throw new ArgumentNullException(nameof(subscription));
            _fetchTimeout = fetchTimeout;

            Redis = redis ?? throw new ArgumentNullException(nameof(redis));
        }

        public RedisClient Redis { get; }

        public override IDisposable AcquireDistributedLock([NotNull] string resource, TimeSpan timeout)
        {
            return Redis.Lock(_storage.GetRedisKey(resource), (int)timeout.TotalSeconds);
        }

        public override void AnnounceServer([NotNull] string serverId, [NotNull] ServerContext context)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));
            if (context == null) throw new ArgumentNullException(nameof(context));

            if (_storage.UseTransactions)
            {
                var transaction = Redis.Multi();

                transaction.SAdd(_storage.GetRedisKey("servers"), serverId);
                transaction.HMSet(_storage.GetRedisKey($"server:{serverId}"), new Dictionary<string, string>
                    {
                        {"WorkerCount", context.WorkerCount.ToString(CultureInfo.InvariantCulture)},
                        {"StartedAt", JobHelper.SerializeDateTime(DateTime.UtcNow)},
                    });

                if (context.Queues.Length > 0)
                {
                    _ = transaction.RPush(
                        _storage.GetRedisKey($"server:{serverId}:queues"),
                        context.Queues);
                }

                _ = transaction.Exec();
            }
            else
            {
                var tasks = new Task[3];
                tasks[0] = Redis.SAddAsync(_storage.GetRedisKey("servers"), serverId);

                tasks[1] = Redis.HMSetAsync(
                    _storage.GetRedisKey($"server:{serverId}"),
                    new Dictionary<string, string>
                        {
                        { "WorkerCount", context.WorkerCount.ToString(CultureInfo.InvariantCulture) },
                        { "StartedAt", JobHelper.SerializeDateTime(DateTime.UtcNow) },
                        });

                if (context.Queues.Length > 0)
                {
                    tasks[2] = Redis.RPushAsync(
                        _storage.GetRedisKey($"server:{serverId}:queues"),
                        context.Queues);
                }
                else
                {
                    tasks[2] = Task.CompletedTask;;
                }

                Task.WaitAll(tasks);
            }
        }

        public override DateTime GetUtcDateTime()
        {
            //the returned time is the time on the first server of the cluster
            return Redis.Time();
        }
        
        public override long GetSetCount([NotNull] IEnumerable<string> keys, int limit)
        {
            Task[] tasks = new Task[keys.Count()];
            int i = 0;
            var batch = Redis.StartPipe();
            ConcurrentDictionary<string, long> results = new ConcurrentDictionary<string, long>();
            foreach (string key in keys)
            {
                tasks[i] = batch.ZCountAsync(_storage.GetRedisKey(key), -1, limit)
                    .ContinueWith((Task<long> x) => results.TryAdd(_storage.GetRedisKey(key), x.Result));
            }
            batch.EndPipe();
            Task.WaitAll(tasks);
            return results.Sum(x => x.Value);
        }
        
        public override bool GetSetContains([NotNull] string key, [NotNull] string value)
        {
            var sortedSetEntries = Redis.ZScan(_storage.GetRedisKey(key), value, 250);
            return sortedSetEntries.Any();
        }
        
        public override string CreateExpiredJob(
            [NotNull] Job job,
            [NotNull] IDictionary<string, string> parameters,
            DateTime createdAt,
            TimeSpan expireIn)
        {
            if (job == null) throw new ArgumentNullException(nameof(job));
            if (parameters == null) throw new ArgumentNullException(nameof(parameters));

            var jobId = Guid.NewGuid().ToString();

            var invocationData = InvocationData.SerializeJob(job);

            // Do not modify the original parameters.
            var storedParameters = new Dictionary<string, string>(parameters)
            {
                { "Type", invocationData.Type },
                { "Method", invocationData.Method },
                { "ParameterTypes", invocationData.ParameterTypes },
                { "Arguments", invocationData.Arguments },
                { "CreatedAt", JobHelper.SerializeDateTime(createdAt) }
            };
            
            if (invocationData.Queue != null)
            {
                storedParameters.Add("Queue", invocationData.Queue);
            }

            if (_storage.UseTransactions)
            {
                var transaction = Redis.Multi();

                _ = transaction.HMSetAsync(_storage.GetRedisKey($"job:{jobId}"), storedParameters);
                _ = transaction.ExpireAsync(_storage.GetRedisKey($"job:{jobId}"), expireIn);

                transaction.Exec();
            }
            else
            {
                var tasks = new Task[2];
                tasks[0] = Redis.HMSetAsync(_storage.GetRedisKey($"job:{jobId}"), storedParameters);
                tasks[1] = Redis.ExpireAsync(_storage.GetRedisKey($"job:{jobId}"), expireIn);
                Task.WaitAll(tasks);
            }

            return jobId;
        }

        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            if (_storage.UseTransactions)
            {
                return new RedisWriteOnlyTransaction(_storage, Redis);
            }
            return new RedisWriteDirectlyToDatabase(_storage, Redis);
        }

        public override void Dispose()
        {
            // nothing to dispose
        }

        public override IFetchedJob FetchNextJob([NotNull] string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException(nameof(queues));

            string jobId = null;
            string queueName = null;
            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                for (int i = 0; i < queues.Length; i++)
                {
                    queueName = queues[i];
                    var queueKey = _storage.GetRedisKey($"queue:{queueName}");
                    var fetchedKey = _storage.GetRedisKey($"queue:{queueName}:dequeued");
                    jobId = Redis.RPopLPush(queueKey, fetchedKey);
                    if (jobId != null) break;
                }

                if (jobId == null)
                {
                    _subscription.WaitForJob(_fetchTimeout, cancellationToken);
                }
            }
            while (jobId == null);

            // The job was fetched by the server. To provide reliability,
            // we should ensure, that the job will be performed and acquired
            // resources will be disposed even if the server will crash
            // while executing one of the subsequent lines of code.

            // The job's processing is splitted into a couple of checkpoints.
            // Each checkpoint occurs after successful update of the
            // job information in the storage. And each checkpoint describes
            // the way to perform the job when the server was crashed after
            // reaching it.

            // Checkpoint #1-1. The job was fetched into the fetched list,
            // that is being inspected by the FetchedJobsWatcher instance.
            // Job's has the implicit 'Fetched' state.

            var fetchTime = DateTime.UtcNow;
            _ = Redis.HSet(
                _storage.GetRedisKey($"job:{jobId}"),
                "Fetched",
                JobHelper.SerializeDateTime(fetchTime));

            // Checkpoint #2. The job is in the implicit 'Fetched' state now.
            // This state stores information about fetched time. The job will
            // be re-queued when the JobTimeout will be expired.

            return new RedisFetchedJob(_storage, Redis, jobId, queueName, fetchTime);
        }

        public override Dictionary<string, string> GetAllEntriesFromHash([NotNull] string key)
        {
            var result = Redis.HGetAll(_storage.GetRedisKey(key));

            return result.Count != 0 ? result : null;
        }

        public override List<string> GetAllItemsFromList([NotNull] string key)
        {
            return Redis.LRange(_storage.GetRedisKey(key), 0, -1).ToList();
        }

        public override HashSet<string> GetAllItemsFromSet([NotNull] string key)
        {
            HashSet<string> result = new HashSet<string>();
            foreach (var item in Redis.ZScan(_storage.GetRedisKey(key), 0, null).items)
            {
                _ = result.Add(item.member);
            }

            return result;
        }

        public override long GetCounter([NotNull] string key)
        {
            return Convert.ToInt64(Redis.Get(_storage.GetRedisKey(key)));
        }

        public override string GetFirstByLowestScoreFromSet([NotNull] string key, double fromScore, double toScore)
        {
            return Redis.ZRangeByScore(_storage.GetRedisKey(key), (decimal)fromScore, (decimal)toScore, offset: 0, count: 1)
                .FirstOrDefault();
        }

        public override List<string> GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore, int count)
        {
            return Redis.ZRangeByScore(_storage.GetRedisKey(key), (decimal)fromScore, (decimal)toScore, offset: 0, count: 1)
                .ToList();
        }

        public override long GetHashCount([NotNull] string key)
        {
            return Redis.HLen(_storage.GetRedisKey(key));
        }

        public override TimeSpan GetHashTtl([NotNull] string key)
        {
            return TimeSpan.FromSeconds(Redis.Ttl(_storage.GetRedisKey(key)));
        }

        public override JobData GetJobData([NotNull] string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            var storedData = Redis.HGetAll(_storage.GetRedisKey($"job:{jobId}"));
            if (storedData.Count == 0) return null;

            string queue = storedData.FirstOrDefault(x => x.Key == "Queue").Value;
            string type = storedData.FirstOrDefault(x => x.Key == "Type").Value;
            string method = storedData.FirstOrDefault(x => x.Key == "Method").Value;
            string parameterTypes = storedData.FirstOrDefault(x => x.Key == "ParameterTypes").Value;
            string arguments = storedData.FirstOrDefault(x => x.Key == "Arguments").Value;
            string createdAt = storedData.FirstOrDefault(x => x.Key == "CreatedAt").Value;

            Job job = null;
            JobLoadException loadException = null;

            var invocationData = new InvocationData(type, method, parameterTypes, arguments, queue);

            try
            {
                job = invocationData.DeserializeJob();
            }
            catch (JobLoadException ex)
            {
                loadException = ex;
            }

            return new JobData
            {
                Job = job,
                State = storedData.FirstOrDefault(x => x.Key == "State").Value,
                CreatedAt = JobHelper.DeserializeNullableDateTime(createdAt) ?? DateTime.MinValue,
                LoadException = loadException
            };
        }

        public override string GetJobParameter([NotNull] string jobId, [NotNull] string name)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));
            if (name == null) throw new ArgumentNullException(nameof(name));

            return Redis.HGet(_storage.GetRedisKey($"job:{jobId}"), name);
        }

        public override long GetListCount([NotNull] string key)
        {
            return Redis.LLen(_storage.GetRedisKey(key));
        }

        public override TimeSpan GetListTtl([NotNull] string key)
        {
            return TimeSpan.FromSeconds(Redis.Ttl(_storage.GetRedisKey(key)));
        }

        public override List<string> GetRangeFromList([NotNull] string key, int startingFrom, int endingAt)
        {
            return Redis.LRange(_storage.GetRedisKey(key), startingFrom, endingAt).ToList();
        }

        public override List<string> GetRangeFromSet([NotNull] string key, int startingFrom, int endingAt)
        {
            return Redis.ZRange(_storage.GetRedisKey(key), startingFrom, endingAt).ToList();
        }

        public override long GetSetCount([NotNull] string key)
        {
            return Redis.ZCard(_storage.GetRedisKey(key));
        }

        public override TimeSpan GetSetTtl([NotNull] string key)
        {
            return TimeSpan.FromSeconds(Redis.Ttl(_storage.GetRedisKey(key)));
        }

        public override StateData GetStateData([NotNull] string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            var entries = Redis.HGetAll(_storage.GetRedisKey($"job:{jobId}:state"));
            if (entries.Count == 0) return null;
            
            entries.TryGetValue("State", out var name);
            entries.TryGetValue("Reason", out var reason);

            _ = entries.Remove("State");
            _ = entries.Remove("Reason");

            return new StateData
            {
                Name = name,
                Reason = reason,
                Data = entries
            };
        }

        public override string GetValueFromHash([NotNull] string key, [NotNull] string name)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));

            return Redis.HGet(_storage.GetRedisKey(key), name);
        }

        public override void Heartbeat([NotNull] string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            _ = Redis.HSet(
                _storage.GetRedisKey($"server:{serverId}"),
                "Heartbeat",
                JobHelper.SerializeDateTime(DateTime.UtcNow));
        }

        public override void RemoveServer([NotNull] string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            if (_storage.UseTransactions)
            {
                var transaction = Redis.Multi();

                _ = transaction.SRem(_storage.GetRedisKey("servers"), serverId);

                _ = transaction.Del(
                    new []
                    {
                    _storage.GetRedisKey($"server:{serverId}"),
                    _storage.GetRedisKey($"server:{serverId}:queues")
                    });

                _ = transaction.Exec();
            }
            else
            {
                var tasks = new Task[2];
                tasks[0] = Redis.SRemAsync(_storage.GetRedisKey("servers"), serverId);
                tasks[1] = Redis.DelAsync(
                    new []
                    {
                        _storage.GetRedisKey($"server:{serverId}"),
                        _storage.GetRedisKey($"server:{serverId}:queues")
                    });
                Task.WaitAll(tasks);
            }
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            var serverNames = Redis.SMembers(_storage.GetRedisKey("servers"));
            var heartbeats = new Dictionary<string, Tuple<DateTime, DateTime?>>();

            var utcNow = DateTime.UtcNow;

            foreach (var serverName in serverNames)
            {
                var srv = Redis.HMGet(_storage.GetRedisKey($"server:{serverName}"), new [] { "StartedAt", "Heartbeat" });
                heartbeats.Add(serverName,
                                new Tuple<DateTime, DateTime?>(
                                JobHelper.DeserializeDateTime(srv[0]),
                                JobHelper.DeserializeNullableDateTime(srv[1])));
            }

            var removedServerCount = 0;
            foreach (var heartbeat in heartbeats)
            {
                var maxTime = new DateTime(
                    Math.Max(heartbeat.Value.Item1.Ticks, (heartbeat.Value.Item2 ?? DateTime.MinValue).Ticks));

                if (utcNow > maxTime.Add(timeOut))
                {
                    RemoveServer(heartbeat.Key);
                    removedServerCount++;
                }
            }

            return removedServerCount;
        }

        public override void SetJobParameter([NotNull] string jobId, [NotNull] string name, string value)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));
            if (name == null) throw new ArgumentNullException(nameof(name));

            _ = Redis.HSet(_storage.GetRedisKey($"job:{jobId}"), name, value);
        }

        public override void SetRangeInHash([NotNull] string key, [NotNull] IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            if (keyValuePairs is Dictionary<string, string> dic)
            {
                Redis.HMSet(_storage.GetRedisKey(key), dic);
            }
            else
            {
                Redis.HMSet(_storage.GetRedisKey(key), keyValuePairs.ToDictionary(x => x.Key, x => x.Value));
            }
        }
    }
}
