using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using NUnit.Framework;
using StackExchange.Redis;

namespace RedisTests;

[TestFixture]
public class SortedSetTests : RedisTestFixture
{
    [Test]
    public async Task CanAddMemberWithScore()
    {
        await Database.SortedSetAddAsync("set", "value", 1);

        var entries = await Database.SortedSetRangeByScoreWithScoresAsync("set");

        entries.Should().HaveCount(1);
        entries.First().Element.ToString().Should().Be("value");
        entries.First().Score.Should().Be(1);
    }

    [Test]
    public async Task AddingExistingMemberUpdatesScore()
    {
        await Database.SortedSetAddAsync("set", "value", 1);
        await Database.SortedSetAddAsync("set", "value", 5);

        var entries = await Database.SortedSetRangeByScoreWithScoresAsync("set");

        entries.Should().HaveCount(1);
        entries.First().Element.ToString().Should().Be("value");
        entries.First().Score.Should().Be(5);
    }

    [Test]
    public async Task CanGetCountOfAllMembers()
    {
        await Database.SortedSetAddAsync("set", "value1", 1);
        await Database.SortedSetAddAsync("set", "value2", 2);

        (await Database.SortedSetLengthAsync("set")).Should().Be(2);
    }

    [Test]
    public async Task CanGetCountOfMembersWithinScoreRange()
    {
        await Database.SortedSetAddAsync("set", "value1", 1);
        await Database.SortedSetAddAsync("set", "value3", 3);
        await Database.SortedSetAddAsync("set", "value5", 5);
        await Database.SortedSetAddAsync("set", "value7", 7);

        (await Database.SortedSetLengthAsync("set", 2, 5)).Should().Be(2);
        (await Database.SortedSetLengthAsync("set", 3, 5, Exclude.Start)).Should().Be(1);
        (await Database.SortedSetLengthAsync("set", 3, 7, Exclude.Stop)).Should().Be(2);
        (await Database.SortedSetLengthAsync("set", 3, 7, Exclude.Both)).Should().Be(1);
    }

    [Test]
    public async Task CanGetCountOfMembersWithinValueRange()
    {
        await Database.SortedSetAddAsync("set", "value1", 1);
        await Database.SortedSetAddAsync("set", "value3", 3);
        await Database.SortedSetAddAsync("set", "value5", 5);
        await Database.SortedSetAddAsync("set", "value7", 7);

        (await Database.SortedSetLengthByValueAsync("set", "value3", "value5")).Should().Be(2);
        (await Database.SortedSetLengthByValueAsync("set", "value3", "value5", Exclude.Start)).Should().Be(1);
        (await Database.SortedSetLengthByValueAsync("set", "value3", "value7", Exclude.Stop)).Should().Be(2);
        (await Database.SortedSetLengthByValueAsync("set", "value3", "value7", Exclude.Both)).Should().Be(1);
    }

    [Test]
    public async Task CanIncrementScoreOfMemberBySpecifiedValue()
    {
        await Database.SortedSetAddAsync("set", "value", 1);

        Database.SortedSetIncrement("set", "value", 1);

        (await Database.SortedSetRangeByScoreWithScoresAsync("set")).First().Score.Should().Be(2);
    }

    [Test]
    public async Task CanRemoveMember()
    {
        await Database.SortedSetAddAsync("set", "value", 1);

        await Database.SortedSetRemoveAsync("set", "value");

        (await Database.SortedSetRangeByValueAsync("set")).Length.Should().Be(0);
    }

    [Test]
    public async Task CanRemoveMembersByScoreRange()
    {
        await Database.SortedSetAddAsync("set", "value1", 1);
        await Database.SortedSetAddAsync("set", "value3", 3);
        await Database.SortedSetAddAsync("set", "value5", 5);
        await Database.SortedSetAddAsync("set", "value7", 7);

        var numRemoved = await Database.SortedSetRemoveRangeByScoreAsync("set", 2, 7);

        numRemoved.Should().Be(3);
        (await Database.SortedSetRangeByScoreAsync("set")).Select(rv => rv.ToString())
            .Should().BeEquivalentTo("value1");
    }

    [Test]
    public async Task CanRemoveMembersByRankRange()
    {
        await Database.SortedSetAddAsync("set", "value1", 1);
        await Database.SortedSetAddAsync("set", "value3", 3);
        await Database.SortedSetAddAsync("set", "value5", 5);
        await Database.SortedSetAddAsync("set", "value7", 7);
        await Database.SortedSetAddAsync("set", "value9", 9);

        var numRemoved = await Database.SortedSetRemoveRangeByRankAsync("set", 1, 3);

        numRemoved.Should().Be(3);
        (await Database.SortedSetRangeByScoreAsync("set")).Select(rv => rv.ToString())
            .Should().BeEquivalentTo("value1", "value9");
    }

    [Test]
    public async Task CanRemoveMembersByValueRange()
    {
        await Database.SortedSetAddAsync("set", "value1", 1);
        await Database.SortedSetAddAsync("set", "value3", 3);
        await Database.SortedSetAddAsync("set", "value5", 5);
        await Database.SortedSetAddAsync("set", "value7", 7);
        await Database.SortedSetAddAsync("set", "value9", 9);

        var numRemoved = await Database.SortedSetRemoveRangeByValueAsync("set", "value3", "value8");

        numRemoved.Should().Be(3);
        (await Database.SortedSetRangeByScoreAsync("set")).Select(rv => rv.ToString())
            .Should().BeEquivalentTo("value1", "value9");
    }

    [Test]
    public async Task CanDetermineRankOrderOfMemberByScore()
    {
        await Database.SortedSetAddAsync("set", "value1", 1);
        await Database.SortedSetAddAsync("set", "value3", 3);
        await Database.SortedSetAddAsync("set", "value5", 5);
        await Database.SortedSetAddAsync("set", "value7", 7);

        (await Database.SortedSetRankAsync("set", "value5", Order.Ascending)).Should().Be(2);
        (await Database.SortedSetRankAsync("set", "value5", Order.Descending)).Should().Be(1);
    }

    [Test]
    public async Task CanGetScoreOfMember()
    {
        await Database.SortedSetAddAsync("set", "value1", 1);

        (await Database.SortedSetScoreAsync("set", "value1")).Should().Be(1);
    }

    [Test]
    public async Task CanStoreIntersectionOfTwoSortedSetsIntoNewOne()
    {
        await Database.SortedSetAddAsync("set1", "value1", 1);
        await Database.SortedSetAddAsync("set1", "value3", 3);
        await Database.SortedSetAddAsync("set1", "value5", 5);
        await Database.SortedSetAddAsync("set1", "value7", 7);

        await Database.SortedSetAddAsync("set2", "value3", 13);
        await Database.SortedSetAddAsync("set2", "value5", 15);
        await Database.SortedSetAddAsync("set2", "value7", 17);
        await Database.SortedSetAddAsync("set2", "value9", 19);

        var setSize = await Database.SortedSetCombineAndStoreAsync(SetOperation.Intersect, "set3", "set1", "set2", Aggregate.Sum);

        setSize.Should().Be(3);
        (await Database.SortedSetRangeByScoreAsync("set3")).Select(rv => rv.ToString())
            .Should().BeEquivalentTo("value3", "value5", "value7");
        (await Database.SortedSetScoreAsync("set3", "value3")).Should().Be(16);
    }

    [Test]
    public async Task CanStoreUnionBetweenTwoSetsIntoNewOne()
    {
        await Database.SortedSetAddAsync("set1", "value1", 1);
        await Database.SortedSetAddAsync("set1", "value3", 3);
        await Database.SortedSetAddAsync("set1", "value5", 5);
        await Database.SortedSetAddAsync("set1", "value7", 7);

        await Database.SortedSetAddAsync("set2", "value3", 13);
        await Database.SortedSetAddAsync("set2", "value5", 15);
        await Database.SortedSetAddAsync("set2", "value7", 17);
        await Database.SortedSetAddAsync("set2", "value9", 19);

        var setSize = await Database.SortedSetCombineAndStoreAsync(SetOperation.Union, "set3", "set1", "set2", Aggregate.Max);

        setSize.Should().Be(5);
        (await Database.SortedSetRangeByScoreAsync("set3")).Select(rv => rv.ToString())
            .Should().BeEquivalentTo("value1", "value3", "value5", "value7", "value9");
        (await Database.SortedSetScoreAsync("set3", "value3")).Should().Be(13);
    }

    // No support for ZPOPMIN and ZPOPMAX yet (newly introduced in Redis 5.0.0)
}