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
            using (var adminConnectionMultiplexer = ConnectionMultiplexer.Connect($"localhost:{RedisSetUp.Port},allowAdmin=true"))
            {
                adminConnectionMultiplexer.GetServer($"localhost:{RedisSetUp.Port}").FlushAllDatabases();
            }

            ConnectionMultiplexer = ConnectionMultiplexer.Connect($"localhost:{RedisSetUp.Port}");
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
