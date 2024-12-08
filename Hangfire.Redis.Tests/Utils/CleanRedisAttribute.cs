using System.Reflection;
using Xunit.Sdk;

namespace Hangfire.Redis.Tests.Utils
{
    public class CleanRedisAttribute : BeforeAfterTestAttribute
    {
        public override void Before(MethodInfo methodUnderTest)
        {
            var client = RedisUtils.RedisClient;
            client.FlushDb();
        }

        public override void After(MethodInfo methodUnderTest)
        {
        }
    }
}