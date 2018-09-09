using System;
using System.Threading;
using NUnit.Framework;
using StackExchange.Redis;

namespace RedisTests
{
    [TestFixture]
    public class KeyValueTests
    {
        private ConnectionMultiplexer _connectionMultiplexer;
        private IDatabase _database;

        [SetUp]
        public void SetUp()
        {
            // Flush database before each test runs.
            using (var adminConnectionMultiplexer = ConnectionMultiplexer.Connect("localhost:6379,allowAdmin=true"))
            {
                adminConnectionMultiplexer.GetServer("localhost:6379").FlushAllDatabases();
            }

            _connectionMultiplexer = ConnectionMultiplexer.Connect("localhost:6379");
            _database = _connectionMultiplexer.GetDatabase();
        }

        [TearDown]
        public void TearDown()
        {
            _database = null;
            _connectionMultiplexer.Dispose();
        }

        [Test]
        public void ValuesAreNullForUnsetKeys()
        {
            Assert.That(_database.StringGet("foo").HasValue, Is.False);
        }

        [Test]
        public void CanGetValueOfKeyAfterSettingIt()
        {
            _database.StringSet("foo", "bar");

            Assert.That(_database.StringGet("foo").Box, Is.EqualTo("bar"));
        }

        [Test]
        public void CanConditionallySetValueIfKeyAlreadyExists()
        {
            _database.StringSet("there", "1");

            _database.StringSet("there", "2", null, When.Exists);
            _database.StringSet("notThere", "1", null, When.Exists);

            Assert.That(_database.StringGet("there").Box, Is.EqualTo("2"));
            Assert.That(_database.KeyExists("notThere"), Is.False);
        }

        [Test]
        public void CanConditionallySetValueIfKeyDoesNotAlreadyExist()
        {
            _database.StringSet("there", "1");

            _database.StringSet("there", "2", null, When.NotExists);
            _database.StringSet("notThere", "1", null, When.NotExists);

            Assert.That(_database.StringGet("there").Box, Is.EqualTo("1"));
            Assert.That(_database.StringGet("notThere").Box, Is.EqualTo("1"));
        }

        [Test]
        public void CanRetrieveExistingValueWhileUpdatingIt()
        {
            _database.StringSet("key", "value1");

            var oldValue = _database.StringGetSet("key", "value2");

            Assert.That(oldValue.Box, Is.EqualTo("value1"));
            Assert.That(_database.StringGet("key").Box, Is.EqualTo("value2"));
        }

        [Test]
        public void CanRemoveExistingKeyValuePair()
        {
            _database.StringSet("key", "value");
            _database.KeyDelete("key");

            Assert.That(_database.KeyExists("key"), Is.False);
        }

        [Test]
        public void CanCheckIfKeyExists()
        {
            _database.StringSet("key", "value");

            Assert.That(_database.KeyExists("key"), Is.True);
        }

        [Test]
        public void CanRenameKey()
        {
            _database.StringSet("key1", "value");
            _database.KeyRename("key1", "key2");

            Assert.That(_database.KeyExists("key1"), Is.False);
            Assert.That(_database.StringGet("key2").Box, Is.EqualTo("value"));
        }

        [Test]
        public void CanSetExpirationPeriodForKey()
        {
            _database.StringSet("key", "value", TimeSpan.FromSeconds(1));

            Assert.That(_database.KeyExists("key"), Is.True);
            Thread.Sleep(TimeSpan.FromSeconds(1));
            Assert.That(_database.KeyExists("key"), Is.False);
        }

        [Test]
        public void CanUpdateExpirationPeriodForKey()
        {
            _database.StringSet("key", "value", TimeSpan.FromSeconds(1));
            _database.KeyExpire("key", TimeSpan.FromSeconds(2));

            Thread.Sleep(TimeSpan.FromSeconds(1));
            Assert.That(_database.StringGet("key").Box, Is.EqualTo("value"));
            Thread.Sleep(TimeSpan.FromSeconds(1));
            Assert.That(_database.KeyExists("key"), Is.False);
        }

        [Test]
        public void CanUpdateExactExpirationTimeForKey()
        {
            _database.StringSet("key", "value", TimeSpan.FromSeconds(1));
            _database.KeyExpire("key", DateTime.Now);

            Assert.That(_database.KeyExists("key"), Is.False);
        }

        [Test]
        public void CanGetTtlForExistingKey()
        {
            _database.StringSet("key", "value", TimeSpan.FromSeconds(1));

            var ttl = _database.KeyTimeToLive("key");

            Assert.That(ttl, Is.Not.Null);
            Assert.That(ttl, Is.LessThanOrEqualTo(TimeSpan.FromSeconds(1)));
        }
    }
}
