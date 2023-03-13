using System.Threading.Tasks;
using FluentAssertions;
using NUnit.Framework;
using StackExchange.Redis;

namespace RedisTests;

[TestFixture]
public class HashTests : RedisTestFixture
{
    [Test]
    public async Task CanSetFieldInHashAndRetrieveIt()
    {
        await Database.HashSetAsync("hash", "foo", "bar");

        (await Database.HashGetAsync("hash", "foo")).ToString().Should().Be("bar");
    }

    [Test]
    public async Task CanSetMultipleFieldsInHashAtOnceAndRetrieveThem()
    {
        await Database.HashSetAsync("hash", new[] {new HashEntry("key1", "value1"), new HashEntry("key2", "value2")});

        var hash = (await Database.HashGetAllAsync("hash")).ToStringDictionary();

        hash.Keys.Should().BeEquivalentTo("key1", "key2");
        hash["key1"].Should().Be("value1");
        hash["key2"].Should().Be("value2");
    }

    [Test]
    public async Task CanConditionallySetValueForFieldThatDoNotAlreadyExist()
    {
        await Database.HashSetAsync("hash", "key1", "value1");

        await Database.HashSetAsync("hash", "key1", "XXX", When.NotExists);
        await Database.HashSetAsync("hash", "key2", "value2", When.NotExists);

        (await Database.HashGetAsync("hash", "key1")).Should().Be("value1");
        (await Database.HashGetAsync("hash", "key2")).Should().Be("value2");
    }

    [Test]
    public async Task CanReplaceValueForExistingField()
    {
        await Database.HashSetAsync("hash", "key", "value1");

        await Database.HashSetAsync("hash", "key", "value2", When.Always);

        (await Database.HashGetAsync("hash", "key")).Should().Be("value2");
    }

    [Test]
    public async Task CanCheckIfFieldExists()
    {
        await Database.HashSetAsync("hash", "key", "value");

        (await Database.HashExistsAsync("hash", "key")).Should().BeTrue();
        (await Database.HashExistsAsync("hash", "XXX")).Should().BeFalse();
    }

    [Test]
    public async Task CanDeleteFieldFromHash()
    {
        await Database.HashSetAsync("hash", "key", "value");

        await Database.HashDeleteAsync("hash", "key");

        (await Database.HashExistsAsync("hash", "key")).Should().BeFalse();
    }

    [Test]
    public async Task CanIncrementValueOfFieldBySpecifiedValue()
    {
        await Database.HashSetAsync("hash", "long", 100);
        await Database.HashSetAsync("hash", "double", 1.00d);

        await Database.HashIncrementAsync("hash", "long", 23);
        await Database.HashIncrementAsync("hash", "double", 0.23d);

        (await Database.HashGetAsync("hash", "long")).Should().Be(123);
        (await Database.HashGetAsync("hash", "double")).Should().Be(1.23d);
    }

    [Test]
    public async Task CanGetAllKeysFromHash()
    {
        await Database.HashSetAsync("hash", "key1", "value1");
        await Database.HashSetAsync("hash", "key2", "value2");

        (await Database.HashKeysAsync("hash")).ToStringArray().Should().BeEquivalentTo("key1", "key2");
    }

    [Test]
    public async Task CanGetAllValuesFromHash()
    {
        await Database.HashSetAsync("hash", "key1", "value1");
        await Database.HashSetAsync("hash", "key2", "value2");

        (await Database.HashValuesAsync("hash")).ToStringArray().Should().BeEquivalentTo("value1", "value2");
    }

    [Test]
    public async Task CanGetNumberOfFieldsInHash()
    {
        await Database.HashSetAsync("hash", "key1", "value1");
        await Database.HashSetAsync("hash", "key2", "value2");

        (await Database.HashLengthAsync("hash")).Should().Be(2);
    }

    [Test]
    public async Task CanGetValuesOfMultipleHashFields()
    {
        await Database.HashSetAsync("hash", "key1", "value1");
        await Database.HashSetAsync("hash", "key2", "value2");
        await Database.HashSetAsync("hash", "key3", "value3");

        (await Database.HashGetAsync("hash", new RedisValue[] {"key1", "key3"})).ToStringArray().Should().BeEquivalentTo("value1", "value3");
    }

    [Test]
    public async Task CanScanThroughFieldsStartingFromOffset()
    {
        await Database.HashSetAsync("hash", "key1", "value1");
        await Database.HashSetAsync("hash", "key2", "value2");
        await Database.HashSetAsync("hash", "key3", "value3");
        await Database.HashSetAsync("hash", "key4", "value4");

        // This behaves rather unexpectedly.  Page size does not matter from the perspective
        // of the API, because the cursor is handled internally so that you can just keep
        // iterating on the result and additional pages will be automatically fetched to keep
        // results coming in.  It also appears that the page offset has nothing to do with
        // the defined page size.  It is treated as the index to start at.
        var results = Database.HashScanAsync("hash", default(RedisValue), pageOffset: 2);

        results.Should().BeEquivalentTo(new[] {new HashEntry("key3", "value3"), new HashEntry("key4", "value4")});
    }

    [Test]
    public async Task CanScanThroughFilteredFieldsStartingFromOffset()
    {
        await Database.HashSetAsync("hash", "abc", "1");
        await Database.HashSetAsync("hash", "abcd", "2");
        await Database.HashSetAsync("hash", "xyz", "3");
        await Database.HashSetAsync("hash", "abcdefg", "4");

        var results = Database.HashScanAsync("hash", "abc*");

        results.Should().BeEquivalentTo(new[] {new HashEntry("abc", "1"), new HashEntry("abcd", "2"), new HashEntry("abcdefg", "4")});
    }
}