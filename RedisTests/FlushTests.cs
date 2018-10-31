using NUnit.Framework;
using StackExchange.Redis;

namespace RedisTests
{
    [TestFixture]
    public class FlushTests
    {
        [Test]
        public void FlushAllDatabasesRemovesEverythingFromRedis()
        {
            using (var connectionMultiplexer = ConnectionMultiplexer.Connect($"localhost:{RedisSetUp.Port},allowAdmin=true"))
            {
                var database = connectionMultiplexer.GetDatabase();
                database.StringSet("foo", "bar");

                var server = connectionMultiplexer.GetServer($"localhost:{RedisSetUp.Port}");
                server.FlushAllDatabases();

                Assert.That(database.KeyExists("foo"), Is.False);
            }
        }
    }
}
