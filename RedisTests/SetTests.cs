using System.Threading.Tasks;
using FluentAssertions;
using NUnit.Framework;
using StackExchange.Redis;

namespace RedisTests;

[TestFixture]
public class SetTests : RedisTestFixture
{
    [Test]
    public async Task CanAddMemberToNewSet()
    {
        await Database.SetAddAsync("set", "1");

        (await Database.SetMembersAsync("set")).ToStringArray().Should().ContainInOrder("1");
    }

    [Test]
    public async Task SetsCanContainUniqueMembers()
    {
        await Database.SetAddAsync("set", "1");
        await Database.SetAddAsync("set", "2");

        (await Database.SetMembersAsync("set")).ToStringArray().Should().ContainInOrder("1", "2");
    }

    [Test]
    public async Task SetsCannotContainDuplicateMembers()
    {
        await Database.SetAddAsync("set", "1");
        await Database.SetAddAsync("set", "1");

        (await Database.SetMembersAsync("set")).ToStringArray().Should().ContainInOrder("1");
    }

    [Test]
    public async Task CanRemoveMemberFromSet()
    {
        await Database.SetAddAsync("set", "1");
        await Database.SetAddAsync("set", "2");

        await Database.SetRemoveAsync("set", "2");

        (await Database.SetMembersAsync("set")).ToStringArray().Should().ContainInOrder("1");
    }

    [Test]
    public async Task CanCheckIfSetHasMember()
    {
        await Database.SetAddAsync("set", "1");
        await Database.SetAddAsync("set", "2");

        (await Database.SetContainsAsync("set", "2")).Should().BeTrue();
    }

    [Test]
    public async Task CanDetermineSizeOfSet()
    {
        await Database.SetAddAsync("set", "1");
        await Database.SetAddAsync("set", "2");

        (await Database.SetLengthAsync("set")).Should().Be(2);
    }

    [Test]
    public async Task CanPopRandomMemberFromSet()
    {
        await Database.SetAddAsync("set", "1");
        await Database.SetAddAsync("set", "2");
        await Database.SetAddAsync("set", "3");

        string popped = Database.SetPop("set").ToString();

        popped.Should().BeOneOf("1", "2", "3");
        Database.SetMembers("set").ToStringArray().Should().NotContain(popped);
    }

    [Test]
    public async Task CanPopSpecifiedNumberOfRandomMembersFromSet()
    {
        await Database.SetAddAsync("set", "1");
        await Database.SetAddAsync("set", "2");
        await Database.SetAddAsync("set", "3");

        var popped = Database.SetPop("set", 2).ToStringArray();

        popped.Should().BeSubsetOf(new[] {"1", "2", "3"});
        Database.SetMembers("set").ToStringArray().Should().NotContain(popped);
    }

    [Test]
    public async Task CanGetDifferenceBetweenTwoSets()
    {
        await Database.SetAddAsync("set1", "1");
        await Database.SetAddAsync("set1", "2");
        await Database.SetAddAsync("set1", "3");
        await Database.SetAddAsync("set1", "4");

        await Database.SetAddAsync("set2", "1");
        await Database.SetAddAsync("set2", "3");

        var difference = (await Database.SetCombineAsync(SetOperation.Difference, "set1", "set2")).ToStringArray();

        difference.Should().BeEquivalentTo("2", "4");
    }

    [Test]
    public async Task CanGetDifferenceBetweenOneSetAndMultipleOthers()
    {
        await Database.SetAddAsync("set1", "1");
        await Database.SetAddAsync("set1", "2");
        await Database.SetAddAsync("set1", "3");
        await Database.SetAddAsync("set1", "4");

        await Database.SetAddAsync("set2", "1");
        await Database.SetAddAsync("set2", "3");

        await Database.SetAddAsync("set3", "4");

        var difference = (await Database.SetCombineAsync(SetOperation.Difference, new RedisKey[] { "set1", "set2", "set3"})).ToStringArray();

        difference.Should().BeEquivalentTo("2");
    }

    [Test]
    public async Task CanStoreDifferenceBetweenSetsIntoNewSet()
    {
        await Database.SetAddAsync("set1", "1");
        await Database.SetAddAsync("set1", "2");
        await Database.SetAddAsync("set1", "3");
        await Database.SetAddAsync("set1", "4");

        await Database.SetAddAsync("set2", "1");
        await Database.SetAddAsync("set2", "3");

        var set3Length = await Database.SetCombineAndStoreAsync(SetOperation.Difference, "set3", "set1", "set2");

        set3Length.Should().Be(2);
        (await Database.SetMembersAsync("set3")).ToStringArray().Should().BeEquivalentTo("2", "4");
    }

    [Test]
    public async Task CanGetIntersectionOfTwoSets()
    {
        await Database.SetAddAsync("set1", "1");
        await Database.SetAddAsync("set1", "2");
        await Database.SetAddAsync("set1", "3");
        await Database.SetAddAsync("set1", "4");

        await Database.SetAddAsync("set2", "1");
        await Database.SetAddAsync("set2", "3");

        var intersection = (await Database.SetCombineAsync(SetOperation.Intersect, "set1", "set2")).ToStringArray();

        intersection.Should().BeEquivalentTo("1", "3");
    }

    [Test]
    public async Task CanGetIntersectionOfMultipleSets()
    {
        await Database.SetAddAsync("set1", "1");
        await Database.SetAddAsync("set1", "2");
        await Database.SetAddAsync("set1", "3");
        await Database.SetAddAsync("set1", "4");

        await Database.SetAddAsync("set2", "1");
        await Database.SetAddAsync("set2", "3");

        await Database.SetAddAsync("set3", "3");
        await Database.SetAddAsync("set3", "4");

        var intersection = (await Database.SetCombineAsync(SetOperation.Intersect, new RedisKey[] { "set1", "set2", "set3" })).ToStringArray();

        intersection.Should().BeEquivalentTo("3");
    }

    [Test]
    public async Task CanStoreIntersectionOfSetsIntoNewSet()
    {
        await Database.SetAddAsync("set1", "1");
        await Database.SetAddAsync("set1", "2");
        await Database.SetAddAsync("set1", "3");
        await Database.SetAddAsync("set1", "4");

        await Database.SetAddAsync("set2", "1");
        await Database.SetAddAsync("set2", "3");

        var set3Length = await Database.SetCombineAndStoreAsync(SetOperation.Intersect, "set3", "set1", "set2");

        set3Length.Should().Be(2);
        (await Database.SetMembersAsync("set3")).ToStringArray().Should().BeEquivalentTo("1", "3");
    }

    [Test]
    public async Task CanGetUnionOfTwoSets()
    {
        await Database.SetAddAsync("set1", "1");
        await Database.SetAddAsync("set1", "2");
        await Database.SetAddAsync("set1", "3");

        await Database.SetAddAsync("set2", "1");
        await Database.SetAddAsync("set2", "4");

        var union = (await Database.SetCombineAsync(SetOperation.Union, "set1", "set2")).ToStringArray();

        union.Should().BeEquivalentTo("1", "2", "3", "4");
    }

    [Test]
    public async Task CanGetUnionOfMultipleSets()
    {
        await Database.SetAddAsync("set1", "1");
        await Database.SetAddAsync("set1", "2");

        await Database.SetAddAsync("set2", "3");
        await Database.SetAddAsync("set2", "5");

        await Database.SetAddAsync("set3", "4");
        await Database.SetAddAsync("set3", "5");

        var union = (await Database.SetCombineAsync(SetOperation.Union, new RedisKey[] { "set1", "set2", "set3" })).ToStringArray();

        union.Should().BeEquivalentTo("1", "2", "3", "4", "5");
    }

    [Test]
    public async Task CanStoreUnionOfSetsIntoNewSet()
    {
        await Database.SetAddAsync("set1", "1");
        await Database.SetAddAsync("set1", "2");

        await Database.SetAddAsync("set2", "1");
        await Database.SetAddAsync("set2", "3");

        var set3Length = await Database.SetCombineAndStoreAsync(SetOperation.Union, "set3", "set1", "set2");

        set3Length.Should().Be(3);
        (await Database.SetMembersAsync("set3")).ToStringArray().Should().BeEquivalentTo("1", "2", "3");
    }

    [Test]
    public async Task CanMoveAMemberFromOneSetToAnother()
    {
        await Database.SetAddAsync("set1", "1");
        await Database.SetAddAsync("set1", "2");

        await Database.SetMoveAsync("set1", "set2", "2");

        (await Database.SetContainsAsync("set1", "2")).Should().BeFalse();
        (await Database.SetContainsAsync("set2", "2")).Should().BeTrue();
    }

    [Test]
    public async Task CanEnumerateOverAllSetMembers()
    {
        await Database.SetAddAsync("set", "1");
        await Database.SetAddAsync("set", "2");
        await Database.SetAddAsync("set", "3");
        await Database.SetAddAsync("set", "4");

        var results = Database.SetScanAsync("set");

        results.Should().BeEquivalentTo(new RedisValue[] {"1", "2", "3", "4"});
    }

    [Test]
    public async Task CanEnumerateSetMembersStartingAtPosition()
    {
        await Database.SetAddAsync("set", "1");
        await Database.SetAddAsync("set", "2");
        await Database.SetAddAsync("set", "3");
        await Database.SetAddAsync("set", "4");

        // This behaves rather unexpectedly.  Page size does not matter from the perspective
        // of the API, because the cursor is handled internally so that you can just keep
        // iterating on the result and additional pages will be automatically fetched to keep
        // results coming in.  It also appears that the page offset has nothing to do with
        // the defined page size.  It is treated as the index to start at.
        var results = Database.SetScanAsync("set", default(RedisValue), pageOffset: 2);

        results.Should().BeEquivalentTo(new RedisValue[] { "3", "4" });
    }
}