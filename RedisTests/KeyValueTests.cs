using System;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using NUnit.Framework;
using StackExchange.Redis;

namespace RedisTests;

[TestFixture]
public class KeyValueTests : RedisTestFixture
{
    [Test]
    public async Task ValuesAreNullForUnsetKeys()
    {
        (await Database.StringGetAsync("foo")).HasValue.Should().BeFalse();
    }

    [Test]
    public async Task CanGetValueOfKeyAfterSettingIt()
    {
        await Database.StringSetAsync("foo", "bar");

        (await Database.StringGetAsync("foo")).Should().Be("bar");
    }

    [Test]
    public async Task CanConditionallySetValueIfKeyAlreadyExists()
    {
        await Database.StringSetAsync("there", "1");

        await Database.StringSetAsync("there", "2", null, When.Exists);
        await Database.StringSetAsync("notThere", "1", null, When.Exists);

        (await Database.StringGetAsync("there")).Should().Be("2");
        (await Database.KeyExistsAsync("notThere")).Should().BeFalse();
    }

    [Test]
    public async Task CanConditionallySetValueIfKeyDoesNotAlreadyExist()
    {
        await Database.StringSetAsync("there", "1");

        await Database.StringSetAsync("there", "2", null, When.NotExists);
        await Database.StringSetAsync("notThere", "1", null, When.NotExists);

        (await Database.StringGetAsync("there")).Should().Be("1");
        (await Database.StringGetAsync("notThere")).Should().Be("1");
    }

    [Test]
    public async Task CanRetrieveExistingValueWhileUpdatingIt()
    {
        await Database.StringSetAsync("key", "value1");

        var oldValue = await Database.StringGetSetAsync("key", "value2");

        oldValue.Should().Be("value1");
        (await Database.StringGetAsync("key")).Should().Be("value2");
    }

    [Test]
    public async Task CanRemoveExistingKeyValuePair()
    {
        await Database.StringSetAsync("key", "value");
        await Database.KeyDeleteAsync("key");

        (await Database.KeyExistsAsync("key")).Should().BeFalse();
    }

    [Test]
    public async Task CanCheckIfKeyExists()
    {
        await Database.StringSetAsync("key", "value");

        (await Database.KeyExistsAsync("key")).Should().BeTrue();
    }

    [Test]
    public async Task CanRenameKey()
    {
        await Database.StringSetAsync("key1", "value");
        await Database.KeyRenameAsync("key1", "key2");

        (await Database.KeyExistsAsync("key1")).Should().BeFalse();
        (await Database.StringGetAsync("key2")).Should().Be("value");
    }

    [Test]
    public async Task CanSetExpirationPeriodForKey()
    {
        await Database.StringSetAsync("key", "value", TimeSpan.FromSeconds(1));

        (await Database.KeyExistsAsync("key")).Should().BeTrue();
        await Task.Delay(TimeSpan.FromSeconds(1));
        (await Database.KeyExistsAsync("key")).Should().BeFalse();
    }

    [Test]
    public async Task CanUpdateExpirationPeriodForKey()
    {
        await Database.StringSetAsync("key", "value", TimeSpan.FromSeconds(1));
        await Database.KeyExpireAsync("key", TimeSpan.FromSeconds(2));

        await Task.Delay(TimeSpan.FromSeconds(1));
        (await Database.StringGetAsync("key")).Should().Be("value");
        await Task.Delay(TimeSpan.FromSeconds(1));
        (await Database.KeyExistsAsync("key")).Should().BeFalse();
    }

    [Test]
    public async Task CanUpdateExactExpirationTimeForKey()
    {
        await Database.StringSetAsync("key", "value", TimeSpan.FromSeconds(1));
        await Database.KeyExpireAsync("key", DateTime.Now);

        (await Database.KeyExistsAsync("key")).Should().BeFalse();
    }

    [Test]
    public async Task CanGetTtlForExistingKey()
    {
        await Database.StringSetAsync("key", "value", TimeSpan.FromSeconds(1));

        var ttl = await Database.KeyTimeToLiveAsync("key");

        ttl.Should().NotBeNull();
        ttl.Should().BeLessOrEqualTo(TimeSpan.FromSeconds(1));
    }
}