using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using NUnit.Framework;
using StackExchange.Redis;

namespace RedisTests;

[TestFixture]
public class ListTests : RedisTestFixture
{
    [Test]
    public async Task CanLeftOrRightPushItemToNewList()
    {
        await Database.ListLeftPushAsync("list1", "1");
        await Database.ListRightPushAsync("list2", "2");

        (await Database.ListLengthAsync("list1")).Should().Be(1);
        (await Database.ListLengthAsync("list2")).Should().Be(1);

        (await Database.ListRangeAsync("list1")).Select(rv => rv.ToString()).Should().BeEquivalentTo("1");
        (await Database.ListRangeAsync("list2")).Select(rv => rv.ToString()).Should().BeEquivalentTo("2");
    }

    [Test]
    public async Task CanGetLengthOfList()
    {
        await Database.ListLeftPushAsync("list", "1");
        await Database.ListLeftPushAsync("list", "2");

        (await Database.ListLengthAsync("list")).Should().Be(2);
    }

    [Test]
    public async Task CanPushNewItemToLeftOfList()
    {
        await Database.ListLeftPushAsync("list", "1");
        await Database.ListLeftPushAsync("list", "2");

        (await Database.ListRangeAsync("list")).ToStringArray().Should().ContainInOrder("2", "1");
    }

    [Test]
    public async Task CanPushNewItemToRightOfList()
    {
        await Database.ListLeftPushAsync("list", "1");
        await Database.ListRightPushAsync("list", "2");

        (await Database.ListRangeAsync("list")).ToStringArray().Should().ContainInOrder("1", "2");
    }

    [Test]
    public async Task CanGetElementOfListByIndex()
    {
        await Database.ListRightPushAsync("list", "1");
        await Database.ListRightPushAsync("list", "2");
        await Database.ListRightPushAsync("list", "3");

        (await Database.ListGetByIndexAsync("list", 1)).Should().Be("2");
    }

    [Test]
    public async Task CanSetElementOfListByIndex()
    {
        await Database.ListRightPushAsync("list", "1");
        await Database.ListRightPushAsync("list", "2");
        await Database.ListRightPushAsync("list", "3");

        await Database.ListSetByIndexAsync("list", 1, "9");

        (await Database.ListGetByIndexAsync("list", 1)).Should().Be("9");
    }

    [Test]
    public async Task CanRemoveAllElementsFromListWithMatchingValue()
    {
        await Database.ListRightPushAsync("list", "1");
        await Database.ListRightPushAsync("list", "2");
        await Database.ListRightPushAsync("list", "3");
        await Database.ListRightPushAsync("list", "2");

        await Database.ListRemoveAsync("list", "2");

        (await Database.ListRangeAsync("list")).ToStringArray().Should().ContainInOrder("1", "3");

    }

    [Test]
    public async Task CanRemoveLimitedNumberOfElementFromListByValue()
    {
        await Database.ListRightPushAsync("list", "2");
        await Database.ListRightPushAsync("list", "1");
        await Database.ListRightPushAsync("list", "2");
        await Database.ListRightPushAsync("list", "3");
        await Database.ListRightPushAsync("list", "2");

        await Database.ListRemoveAsync("list", "2", 2);

        (await Database.ListRangeAsync("list")).ToStringArray().Should().ContainInOrder("1", "3", "2");
    }

    [Test]
    public async Task CanInsertElementIntoListBeforeFirstInstanceOfSpecifiedExistingValue()
    {
        await Database.ListRightPushAsync("list", "1");
        await Database.ListRightPushAsync("list", "2");
        await Database.ListRightPushAsync("list", "3");
        await Database.ListRightPushAsync("list", "2");

        await Database.ListInsertBeforeAsync("list", "2", "9");

        (await Database.ListRangeAsync("list")).ToStringArray().Should().ContainInOrder("1", "9", "2", "3", "2");
    }


    [Test]
    public async Task CanInsertElementIntoListAfterFirstInstanceOfSpecifiedExistingValue()
    {
        await Database.ListRightPushAsync("list", "1");
        await Database.ListRightPushAsync("list", "2");
        await Database.ListRightPushAsync("list", "3");
        await Database.ListRightPushAsync("list", "2");

        await Database.ListInsertAfterAsync("list", "2", "9");

        (await Database.ListRangeAsync("list")).ToStringArray().Should().ContainInOrder("1", "2", "9", "3", "2");
    }

    [Test]
    public async Task CanPopElementFromLeftOfList()
    {
        await Database.ListRightPushAsync("list", "1");
        await Database.ListRightPushAsync("list", "2");

        (await Database.ListLeftPopAsync("list")).Should().Be("1");
        (await Database.ListRangeAsync("list")).ToStringArray().Should().ContainInOrder("2");
    }

    [Test]
    public async Task CanPopElementFromRightOfList()
    {
        await Database.ListRightPushAsync("list", "1");
        await Database.ListRightPushAsync("list", "2");

        (await Database.ListRightPopAsync("list")).Should().Be("2");
        (await Database.ListRangeAsync("list")).ToStringArray().Should().ContainInOrder("1");
    }

    [Test]
    public async Task CanPushValuePoppedFromOneListToAnother()
    {
        await Database.ListRightPushAsync("list1", "1");
        await Database.ListRightPushAsync("list1", "2");

        await Database.ListRightPushAsync("list2", "3");
        await Database.ListRightPushAsync("list2", "4");

        await Database.ListRightPopLeftPushAsync("list1", "list2");

        (await Database.ListRangeAsync("list1")).ToStringArray().Should().ContainInOrder("1");
        (await Database.ListRangeAsync("list2")).ToStringArray().Should().ContainInOrder("2", "3", "4");
    }

    [Test]
    public async Task CanTrimListToRetainOnlySpecifiedRange()
    {
        await Database.ListRightPushAsync("list", "1");
        await Database.ListRightPushAsync("list", "2");
        await Database.ListRightPushAsync("list", "3");
        await Database.ListRightPushAsync("list", "4");
        await Database.ListRightPushAsync("list", "5");

        await Database.ListTrimAsync("list", 1, 3);

        (await Database.ListRangeAsync("list")).ToStringArray().Should().ContainInOrder("2", "3", "4");
    }

    // Blocking operations are not directly implemented by StackExchange.Redis
}