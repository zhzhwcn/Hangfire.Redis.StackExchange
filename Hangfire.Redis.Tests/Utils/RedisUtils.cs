using System;
using FreeRedis;

namespace Hangfire.Redis.Tests.Utils
{
    public static class RedisUtils
    {
        private const string HostVariable = "Hangfire_Redis_Host";
        private const string PortVariable = "Hangfire_Redis_Port";
        private const string DbVariable = "Hangfire_Redis_Db";

        private const string DefaultHost = "127.0.0.1";
        private const int DefaultPort = 6379;
        private const int DefaultDb = 1;


        static RedisUtils()
        {
            RedisClient =
                new RedisClient(GetHostAndPort());

        }
        public static RedisClient RedisClient { get; }

        public static string GetHostAndPort()
        {
            return $"{GetHost()}:{GetPort()},defaultDatabase={GetDb()},connectTimeout=30000,poolsize=100";
        }

        public static string GetHost()
        {
            return Environment.GetEnvironmentVariable(HostVariable)
                   ?? DefaultHost;
        }

        public static int GetPort()
        {
            var portValue = Environment.GetEnvironmentVariable(PortVariable);
            return portValue != null ? int.Parse(portValue) : DefaultPort;
        }

        public static int GetDb()
        {
            var dbValue = Environment.GetEnvironmentVariable(DbVariable);
            return dbValue != null ? int.Parse(dbValue) : DefaultDb;
        }
    }
}