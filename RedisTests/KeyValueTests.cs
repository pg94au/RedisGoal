using System;
using System.Threading;
using FluentAssertions;
using NUnit.Framework;
using StackExchange.Redis;

namespace RedisTests
{
    public class KeyValueTests : RedisTestFixture
    {
        [Test]
        public void ValuesAreNullForUnsetKeys()
        {
            Database.StringGet("foo").HasValue.Should().BeFalse();
        }

        [Test]
        public void CanGetValueOfKeyAfterSettingIt()
        {
            Database.StringSet("foo", "bar");

            Database.StringGet("foo").Should().Be("bar");
        }

        [Test]
        public void CanConditionallySetValueIfKeyAlreadyExists()
        {
            Database.StringSet("there", "1");

            Database.StringSet("there", "2", null, When.Exists);
            Database.StringSet("notThere", "1", null, When.Exists);

            Database.StringGet("there").Should().Be("2");
            Database.KeyExists("notThere").Should().BeFalse();
        }

        [Test]
        public void CanConditionallySetValueIfKeyDoesNotAlreadyExist()
        {
            Database.StringSet("there", "1");

            Database.StringSet("there", "2", null, When.NotExists);
            Database.StringSet("notThere", "1", null, When.NotExists);

            Database.StringGet("there").Should().Be("1");
            Database.StringGet("notThere").Should().Be("1");
        }

        [Test]
        public void CanRetrieveExistingValueWhileUpdatingIt()
        {
            Database.StringSet("key", "value1");

            var oldValue = Database.StringGetSet("key", "value2");

            oldValue.Should().Be("value1");
            Database.StringGet("key").Should().Be("value2");
        }

        [Test]
        public void CanRemoveExistingKeyValuePair()
        {
            Database.StringSet("key", "value");
            Database.KeyDelete("key");

            Database.KeyExists("key").Should().BeFalse();
        }

        [Test]
        public void CanCheckIfKeyExists()
        {
            Database.StringSet("key", "value");

            Database.KeyExists("key").Should().BeTrue();
        }

        [Test]
        public void CanRenameKey()
        {
            Database.StringSet("key1", "value");
            Database.KeyRename("key1", "key2");

            Database.KeyExists("key1").Should().BeFalse();
            Database.StringGet("key2").Should().Be("value");
        }

        [Test]
        public void CanSetExpirationPeriodForKey()
        {
            Database.StringSet("key", "value", TimeSpan.FromSeconds(1));

            Database.KeyExists("key").Should().BeTrue();
            Thread.Sleep(TimeSpan.FromSeconds(1));
            Database.KeyExists("key").Should().BeFalse();
        }

        [Test]
        public void CanUpdateExpirationPeriodForKey()
        {
            Database.StringSet("key", "value", TimeSpan.FromSeconds(1));
            Database.KeyExpire("key", TimeSpan.FromSeconds(2));

            Thread.Sleep(TimeSpan.FromSeconds(1));
            Database.StringGet("key").Should().Be("value");
            Thread.Sleep(TimeSpan.FromSeconds(1));
            Database.KeyExists("key").Should().BeFalse();
        }

        [Test]
        public void CanUpdateExactExpirationTimeForKey()
        {
            Database.StringSet("key", "value", TimeSpan.FromSeconds(1));
            Database.KeyExpire("key", DateTime.Now);

            Database.KeyExists("key").Should().BeFalse();
        }

        [Test]
        public void CanGetTtlForExistingKey()
        {
            Database.StringSet("key", "value", TimeSpan.FromSeconds(1));

            var ttl = Database.KeyTimeToLive("key");

            ttl.Should().NotBeNull();
            ttl.Should().BeLessOrEqualTo(TimeSpan.FromSeconds(1));
        }
    }
}
