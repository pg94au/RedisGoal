using System;
using NUnit.Framework;
using StackExchange.Redis;

namespace RedisTests
{
    [TestFixture]
    public class RedisTestFixture
    {
        protected ConnectionMultiplexer ConnectionMultiplexer;
        protected IDatabase Database;

        [SetUp]
        public void SetUp()
        {
            // Flush database before each test runs.
            using (var adminConnectionMultiplexer = ConnectionMultiplexer.Connect("localhost:6379,allowAdmin=true"))
            {
                adminConnectionMultiplexer.GetServer("localhost:6379").FlushAllDatabases();
            }

            ConnectionMultiplexer = ConnectionMultiplexer.Connect("localhost:6379");
            Database = ConnectionMultiplexer.GetDatabase();
        }

        [TearDown]
        public void TearDown()
        {
            Database = null;
            ConnectionMultiplexer.Dispose();
        }
    }
}
