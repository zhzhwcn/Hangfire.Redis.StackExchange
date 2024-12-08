using System;
using FreeRedis;
using Hangfire.Common;
using Hangfire.Redis.StackExchange;
using Hangfire.Redis.Tests.Utils;
using Moq;
using Xunit;

namespace Hangfire.Redis.Tests
{
    [Collection("Sequential")]
    public class RedisFetchedJobFacts
    {
        private const string JobId = "id";
        private const string Queue = "queue";

        private readonly RedisStorage _storage;
        private readonly RedisClient _redis;

        public RedisFetchedJobFacts()
        {
            _redis = RedisUtils.RedisClient;

            var options = new RedisStorageOptions() { };
            _storage = new RedisStorage(RedisUtils.RedisClient, options);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
           Assert.Throws<ArgumentNullException>("storage",
               () => new RedisFetchedJob(null, _redis, JobId, Queue, DateTime.UtcNow));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenRedisIsNull()
        {
            Assert.Throws<ArgumentNullException>("redis",
                () => new RedisFetchedJob(_storage, null, JobId, Queue, DateTime.UtcNow));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenJobIdIsNull()
        {
            Assert.Throws<ArgumentNullException>("jobId",
                () => new RedisFetchedJob(_storage, _redis, null, Queue, DateTime.UtcNow));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenQueueIsNull()
        {
            Assert.Throws<ArgumentNullException>("queue",
                () => new RedisFetchedJob(_storage, _redis, JobId, null, DateTime.UtcNow));
        }

        [Fact, CleanRedis]
        public void RemoveFromQueue_RemovesJobFromTheFetchedList()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.RPush("{hangfire}:queue:my-queue:dequeued", "job-id");
                
                var fetchedAt = DateTime.UtcNow;
                redis.HSet("{hangfire}:job:job-id", "Fetched", JobHelper.SerializeDateTime(fetchedAt));
                var fetchedJob = new RedisFetchedJob(_storage, redis, "job-id", "my-queue", fetchedAt);

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                Assert.Equal(0, redis.LLen("{hangfire}:queue:my-queue:dequeued"));
            });
        }

        [Fact, CleanRedis]
        public void RemoveFromQueue_RemovesOnlyJobWithTheSpecifiedId()
        {
            UseRedis(redis =>
            {
                // Arrange
				redis.RPush("{hangfire}:queue:my-queue:dequeued", "job-id");
                redis.RPush("{hangfire}:queue:my-queue:dequeued", "another-job-id");

                var fetchedAt = DateTime.UtcNow;
                redis.HSet("{hangfire}:job:job-id", "Fetched", JobHelper.SerializeDateTime(fetchedAt));
                redis.HSet("{hangfire}:job:another-job-id", "Fetched", JobHelper.SerializeDateTime(fetchedAt));
                var fetchedJob = new RedisFetchedJob(_storage, redis, "job-id", "my-queue", fetchedAt);

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                Assert.Equal(1, redis.LLen("{hangfire}:queue:my-queue:dequeued"));
                Assert.Equal("another-job-id", (string)redis.RPop("{hangfire}:queue:my-queue:dequeued"));
            });
        }

        [Fact, CleanRedis]
        public void RemoveFromQueue_DoesNotRemoveIfFetchedDoesntMatch()
        {
            UseRedis(redis =>
            {
                // Arrange
				redis.RPush("{hangfire}:queue:my-queue:dequeued", "job-id");

                var fetchedAt = DateTime.UtcNow;
                redis.HSet("{hangfire}:job:job-id", "Fetched", JobHelper.SerializeDateTime(fetchedAt));
                var fetchedJob = new RedisFetchedJob(_storage, redis, "job-id", "my-queue", fetchedAt + TimeSpan.FromSeconds(1));

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                Assert.Equal(1, redis.LLen("{hangfire}:queue:my-queue:dequeued"));
                Assert.Equal("job-id", (string)redis.RPop("{hangfire}:queue:my-queue:dequeued"));
            });
        }

        [Fact, CleanRedis]
        public void RemoveFromQueue_RemovesTheFetchedFlag()
        {
            UseRedis(redis =>
            {
                // Arrange
                var fetchedAt = DateTime.UtcNow;
                redis.HSet("{hangfire}:job:my-job", "Fetched", JobHelper.SerializeDateTime(fetchedAt));
                var fetchedJob = new RedisFetchedJob(_storage, redis, "my-job", "my-queue", fetchedAt);

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                Assert.False(redis.HExists("{hangfire}:job:my-job", "Fetched"));
            });
        }

        [Fact, CleanRedis]
        public void RemoveFromQueue_RemovesTheCheckedFlag()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.HSet("{hangfire}:job:my-job", "Checked", JobHelper.SerializeDateTime(DateTime.UtcNow));
                var fetchedJob = new RedisFetchedJob(_storage, redis, "my-job", "my-queue", null);

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                Assert.False(redis.HExists("{hangfire}:job:my-job", "Checked"));
            });
        }

        [Fact, CleanRedis]
        public void Requeue_PushesAJobBackToQueue()
        {
            UseRedis(redis => 
            {
                // Arrange
                redis.RPush("{hangfire}:queue:my-queue:dequeued", "my-job");
                var fetchedAt = DateTime.UtcNow;
                redis.HSet("{hangfire}:job:my-job", "Fetched", JobHelper.SerializeDateTime(fetchedAt));

                var fetchedJob = new RedisFetchedJob(_storage, redis, "my-job", "my-queue", fetchedAt);

                // Act
                fetchedJob.Requeue();

                // Assert
                Assert.Equal("my-job", (string)redis.RPop("{hangfire}:queue:my-queue"));
                Assert.Null((string)redis.LPop("{hangfire}:queue:my-queue:dequeued"));
            });
        }

        [Fact, CleanRedis]
        public void Requeue_PushesAJobToTheRightSide()
        {
            UseRedis(redis =>
            {
                // Arrange
				redis.RPush("{hangfire}:queue:my-queue", "another-job");
                redis.RPush("{hangfire}:queue:my-queue:dequeued", "my-job");
                var fetchedAt = DateTime.UtcNow;
                redis.HSet("{hangfire}:job:my-job", "Fetched", JobHelper.SerializeDateTime(fetchedAt));


                var fetchedJob = new RedisFetchedJob(_storage, redis, "my-job", "my-queue", fetchedAt);

                // Act
                fetchedJob.Requeue();

                // Assert - RPOP
                Assert.Equal("my-job", (string)redis.RPop("{hangfire}:queue:my-queue"));
                Assert.Null((string)redis.RPop("{hangfire}:queue:my-queue:dequeued"));
            });
        }

        [Fact, CleanRedis]
        public void Requeue_RemovesAJobFromFetchedList()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.RPush("{hangfire}:queue:my-queue:dequeued", "my-job");
                var fetchedAt = DateTime.UtcNow;
                redis.HSet("{hangfire}:job:my-job", "Fetched", JobHelper.SerializeDateTime(fetchedAt));

                var fetchedJob = new RedisFetchedJob(_storage, redis, "my-job", "my-queue", fetchedAt);

                // Act
                fetchedJob.Requeue();

                // Assert
                Assert.Equal(0, redis.LLen("{hangfire}:queue:my-queue:dequeued"));
            });
        }

        [Fact, CleanRedis]
        public void Requeue_RemovesTheFetchedFlag()
        {
            UseRedis(redis =>
            {
                // Arrange
                var fetchedAt = DateTime.UtcNow;
                redis.HSet("{hangfire}:job:my-job", "Fetched", JobHelper.SerializeDateTime(fetchedAt));
                var fetchedJob = new RedisFetchedJob(_storage, redis, "my-job", "my-queue", fetchedAt);

                // Act
                fetchedJob.Requeue();

                // Assert
                Assert.False(redis.HExists("{hangfire}:job:my-job", "Fetched"));
            });
        }

        [Fact, CleanRedis]
        public void Requeue_RemovesTheCheckedFlag()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.HSet("{hangfire}:job:my-job", "Checked", JobHelper.SerializeDateTime(DateTime.UtcNow));
                var fetchedJob = new RedisFetchedJob(_storage, redis, "my-job", "my-queue", null);

                // Act
                fetchedJob.Requeue();

                // Assert
                Assert.False(redis.HExists("{hangfire}:job:my-job", "Checked"));
            });
        }

        [Fact, CleanRedis]
        public void Dispose_WithNoComplete_RequeuesAJob()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.RPush("{hangfire}:queue:my-queue:dequeued", "my-job");
                var fetchedJob = new RedisFetchedJob(_storage, redis, "my-job", "my-queue", DateTime.UtcNow);

                // Act
                fetchedJob.Dispose();

                // Assert
                Assert.Equal(1, redis.LLen("{hangfire}:queue:my-queue"));
            });
        }

        [Fact, CleanRedis]
        public void Dispose_AfterRemoveFromQueue_DoesNotRequeueAJob()
        {
            UseRedis(redis =>
            {
                // Arrange
				redis.RPush("{hangfire}:queue:my-queue:dequeued", "my-job");
                redis.RPush("{hangfire}:queue:my-queue:dequeued", "my-job");
                var fetchedJob = new RedisFetchedJob(_storage, redis, "my-job", "my-queue", DateTime.UtcNow);

                // Act
                fetchedJob.RemoveFromQueue();
                fetchedJob.Dispose();

                // Assert
                Assert.Equal(0, redis.LLen("{hangfire}:queue:my-queue"));
            });
        }

        private static void UseRedis(Action<RedisClient> action)
        {
			var redis = RedisUtils.RedisClient;
            action(redis);
        }
    }
}
