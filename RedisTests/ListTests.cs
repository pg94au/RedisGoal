using System.Linq;
using FluentAssertions;
using NUnit.Framework;
using StackExchange.Redis;

namespace RedisTests
{
    [TestFixture]
    public class ListTests : RedisTestFixture
    {
        [Test]
        public void CanLeftOrRightPushItemToNewList()
        {
            Database.ListLeftPush("list1", "1");
            Database.ListRightPush("list2", "2");

            Database.ListLength("list1").Should().Be(1);
            Database.ListLength("list2").Should().Be(1);

            Database.ListRange("list1").Select(rv => rv.ToString()).Should().BeEquivalentTo("1");
            Database.ListRange("list2").Select(rv => rv.ToString()).Should().BeEquivalentTo("2");
        }

        [Test]
        public void CanGetLengthOfList()
        {
            Database.ListLeftPush("list", "1");
            Database.ListLeftPush("list", "2");

            Database.ListLength("list").Should().Be(2);
        }

        [Test]
        public void CanPushNewItemToLeftOfList()
        {
            Database.ListLeftPush("list", "1");
            Database.ListLeftPush("list", "2");

            Database.ListRange("list").ToStringArray().Should().ContainInOrder("2", "1");
        }

        [Test]
        public void CanPushNewItemToRightOfList()
        {
            Database.ListLeftPush("list", "1");
            Database.ListRightPush("list", "2");

            Database.ListRange("list").ToStringArray().Should().ContainInOrder("1", "2");
        }

        [Test]
        public void CanGetElementOfListByIndex()
        {
            Database.ListRightPush("list", "1");
            Database.ListRightPush("list", "2");
            Database.ListRightPush("list", "3");

            Database.ListGetByIndex("list", 1).Should().Be("2");
        }

        [Test]
        public void CanSetElementOfListByIndex()
        {
            Database.ListRightPush("list", "1");
            Database.ListRightPush("list", "2");
            Database.ListRightPush("list", "3");

            Database.ListSetByIndex("list", 1, "9");

            Database.ListGetByIndex("list", 1).Should().Be("9");
        }

        [Test]
        public void CanRemoveAllElementsFromListWithMatchingValue()
        {
            Database.ListRightPush("list", "1");
            Database.ListRightPush("list", "2");
            Database.ListRightPush("list", "3");
            Database.ListRightPush("list", "2");

            Database.ListRemove("list", "2");

            Database.ListRange("list").ToStringArray().Should().ContainInOrder("1", "3");

        }

        [Test]
        public void CanRemoveLimitedNumberOfElementFromListByValue()
        {
            Database.ListRightPush("list", "2");
            Database.ListRightPush("list", "1");
            Database.ListRightPush("list", "2");
            Database.ListRightPush("list", "3");
            Database.ListRightPush("list", "2");

            Database.ListRemove("list", "2", 2);

            Database.ListRange("list").ToStringArray().Should().ContainInOrder("1", "3", "2");
        }

        [Test]
        public void CanInsertElementIntoListBeforeFirstInstanceOfSpecifiedExistingValue()
        {
            Database.ListRightPush("list", "1");
            Database.ListRightPush("list", "2");
            Database.ListRightPush("list", "3");
            Database.ListRightPush("list", "2");

            Database.ListInsertBefore("list", "2", "9");

            Database.ListRange("list").ToStringArray().Should().ContainInOrder("1", "9", "2", "3", "2");
        }


        [Test]
        public void CanInsertElementIntoListAfterFirstInstanceOfSpecifiedExistingValue()
        {
            Database.ListRightPush("list", "1");
            Database.ListRightPush("list", "2");
            Database.ListRightPush("list", "3");
            Database.ListRightPush("list", "2");

            Database.ListInsertAfter("list", "2", "9");

            Database.ListRange("list").ToStringArray().Should().ContainInOrder("1", "2", "9", "3", "2");
        }

        [Test]
        public void CanPopElementFromLeftOfList()
        {
            Database.ListRightPush("list", "1");
            Database.ListRightPush("list", "2");

            Database.ListLeftPop("list").Should().Be("1");
            Database.ListRange("list").ToStringArray().Should().ContainInOrder("2");
        }

        [Test]
        public void CanPopElementFromRightOfList()
        {
            Database.ListRightPush("list", "1");
            Database.ListRightPush("list", "2");

            Database.ListRightPop("list").Should().Be("2");
            Database.ListRange("list").ToStringArray().Should().ContainInOrder("1");
        }

        [Test]
        public void CanPushValuePoppedFromOneListToAnother()
        {
            Database.ListRightPush("list1", "1");
            Database.ListRightPush("list1", "2");

            Database.ListRightPush("list2", "3");
            Database.ListRightPush("list2", "4");

            Database.ListRightPopLeftPush("list1", "list2");

            Database.ListRange("list1").ToStringArray().Should().ContainInOrder("1");
            Database.ListRange("list2").ToStringArray().Should().ContainInOrder("2", "3", "4");
        }

        [Test]
        public void CanTrimListToRetainOnlySpecifiedRange()
        {
            Database.ListRightPush("list", "1");
            Database.ListRightPush("list", "2");
            Database.ListRightPush("list", "3");
            Database.ListRightPush("list", "4");
            Database.ListRightPush("list", "5");

            Database.ListTrim("list", 1, 3);

            Database.ListRange("list").ToStringArray().Should().ContainInOrder("2", "3", "4");
        }

        // Blocking operations are not directly implemented by StackExchange.Redis
    }
}
