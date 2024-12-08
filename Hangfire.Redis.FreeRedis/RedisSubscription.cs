﻿using System;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Server;
using FreeRedis;

namespace Hangfire.Redis.StackExchange
{
#pragma warning disable 618
    internal class RedisSubscription : IServerComponent
#pragma warning restore 618
    {
        private readonly ManualResetEvent _mre = new ManualResetEvent(false);
        private readonly RedisStorage _storage;
        private readonly RedisClient _redisClient;

        public RedisSubscription([NotNull] RedisStorage storage, [NotNull] RedisClient redisClient)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            Channel = _storage.GetRedisKey("JobFetchChannel");
            _redisClient = redisClient ?? throw new ArgumentNullException(nameof(redisClient));
            
        }

        public string Channel { get; }

        public void WaitForJob(TimeSpan timeout, CancellationToken cancellationToken)
        {
            _mre.Reset();
            WaitHandle.WaitAny(new[] {_mre, cancellationToken.WaitHandle}, timeout);
        }

        void IServerComponent.Execute(CancellationToken cancellationToken)
        {
            _redisClient.Subscribe(Channel, (channel, value) => _mre.Set());
            cancellationToken.WaitHandle.WaitOne();

            if (cancellationToken.IsCancellationRequested)
            {
                _redisClient.UnSubscribe(Channel);
                _mre.Reset();
            }
        }

        ~RedisSubscription()
        {
            _mre.Dispose();
        }
    }
}