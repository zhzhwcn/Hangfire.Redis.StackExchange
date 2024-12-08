using FreeRedis;
using Hangfire.Redis.Tests.Utils;
using Xunit;

namespace Hangfire.Redis.Tests
{
    [Collection("Sequential")]
    public class RedisTest 
    {
        private readonly RedisClient _redis;

        public RedisTest()
        {
            _redis = RedisUtils.RedisClient;
        }


        [Fact, CleanRedis]
        public void RedisSampleTest()
        {
            var defaultValue = _redis.Get("samplekey");
            Assert.True(string.IsNullOrEmpty(defaultValue));
        }
    }
}