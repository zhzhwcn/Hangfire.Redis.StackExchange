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
using FreeRedis;
using Hangfire.Annotations;
using Hangfire.Dashboard;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;


namespace Hangfire.Redis.StackExchange
{
    public class RedisStorage : JobStorage
    {
        // Make sure in Redis Cluster all transaction are in the same slot !!
        private readonly RedisStorageOptions _options;
        private readonly RedisClient _redisClient;
        private readonly RedisSubscription _subscription;

        private readonly Dictionary<string, bool> _features =
            new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase)
            {
                { JobStorageFeatures.ExtendedApi, true }, 
                { JobStorageFeatures.JobQueueProperty, true }, 
                { JobStorageFeatures.Connection.BatchedGetFirstByLowest, true }, 
                { JobStorageFeatures.Connection.GetUtcDateTime, true }, 
                { JobStorageFeatures.Connection.GetSetContains, true }, 
                { JobStorageFeatures.Connection.LimitedGetSetCount, true }, 
                { JobStorageFeatures.Transaction.AcquireDistributedLock, true }, 
                { JobStorageFeatures.Transaction.CreateJob, true }, // overridden in constructor
                { JobStorageFeatures.Transaction.SetJobParameter, true}, // overridden in constructor 
                { JobStorageFeatures.Transaction.RemoveFromQueue(typeof(RedisFetchedJob)), true }, // overridden in constructor
                { JobStorageFeatures.Monitoring.DeletedStateGraphs, true }, 
                { JobStorageFeatures.Monitoring.AwaitingJobs, true }
            };

        public RedisStorage()
            : this("localhost:6379", null)
        {
        }

        public RedisStorage(string connectionString, RedisStorageOptions options = null)
            : this(new RedisClient(connectionString), options)
        {
        }

        public RedisStorage(RedisClient redisClient, RedisStorageOptions options = null)
        {
            if (redisClient == null)
                throw new ArgumentNullException(nameof(redisClient));

            _redisClient = redisClient;

            _options = options ?? new RedisStorageOptions
            {
                
            };

            SetTransactionalFeatures();

            _subscription = new RedisSubscription(this, _redisClient);
        }

        private void SetTransactionalFeatures()
        {
            _features[JobStorageFeatures.Transaction.CreateJob] = _options.UseTransactions;
            _features[JobStorageFeatures.Transaction.SetJobParameter] = _options.UseTransactions;
            _features[JobStorageFeatures.Transaction.RemoveFromQueue(typeof(RedisFetchedJob))] = _options.UseTransactions; 
        }

        
        internal int SucceededListSize => _options.SucceededListSize;

        internal int DeletedListSize => _options.DeletedListSize;
        
        internal string SubscriptionChannel => _subscription.Channel;

        internal string[] LifoQueues => _options.LifoQueues;

        internal bool UseTransactions => _options.UseTransactions;

        public override IMonitoringApi GetMonitoringApi()
        {
            return new RedisMonitoringApi(this, _redisClient);
        }
        public override bool HasFeature([NotNull] string featureId)
        {
            if (featureId == null) throw new ArgumentNullException(nameof(featureId));

            return _features.TryGetValue(featureId, out var isSupported)
                ? isSupported
                : base.HasFeature(featureId);

        }
        public override IStorageConnection GetConnection()
        {
            return new RedisConnection(this, _redisClient, _subscription, _options.FetchTimeout);
        }

#pragma warning disable 618
        public override IEnumerable<IServerComponent> GetComponents()
#pragma warning restore 618
        {
            yield return new FetchedJobsWatcher(this, _options.InvisibilityTimeout);
            yield return new ExpiredJobsWatcher(this, _options.ExpiryCheckInterval);
            yield return _subscription;
        }

        public static DashboardMetric GetDashboardMetricFromRedisInfo(string title, string key)
        {
            return new DashboardMetric("redis:" + key, title, (razorPage) =>
            {
                using (var redisCnn = razorPage.Storage.GetConnection())
                {
                    var db = (redisCnn as RedisConnection).Redis;
                    var rawInfo = db.Info().Split('\n')
                        .Where(x => x.Contains(':'))
                        .ToDictionary(x => x.Split(':')[0], x => x.Split(':')[1]);

                    return new Metric(rawInfo[key]);
                }
            });
        }

        public override IEnumerable<IStateHandler> GetStateHandlers()
        {
            yield return new FailedStateHandler();
            yield return new ProcessingStateHandler();
            yield return new SucceededStateHandler();
            yield return new DeletedStateHandler();
        }

        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Debug("Using the following options for Redis job storage:");

            logger.DebugFormat("ConnectionString: {0}", _redisClient);
        }

        public override string ToString()
        {
            return $"redis://{_redisClient}";
        }

        internal string GetRedisKey([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _options.Prefix + key;
        }
    }
}

