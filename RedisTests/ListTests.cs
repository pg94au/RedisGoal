using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using NUnit.Framework;
using StackExchange.Redis;

namespace RedisTests
{
    [TestFixture]
    public class ListTests
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
        public void CanLeftOrRightPushItemToNewList()
        {
            _database.ListLeftPush("list1", "1");
            _database.ListRightPush("list2", "2");

            Assert.That(_database.ListLength("list1"), Is.EqualTo(1));
            Assert.That(_database.ListLength("list2"), Is.EqualTo(1));

            Assert.That(_database.ListRange("list1").Select(rv => rv.Box()), Is.EquivalentTo(new[] { "1" }));
            Assert.That(_database.ListRange("list2").Select(rv => rv.Box()), Is.EquivalentTo(new[] { "2" }));
        }

        [Test]
        public void CanGetLengthOfList()
        {
            _database.ListLeftPush("list", "1");
            _database.ListLeftPush("list", "2");

            Assert.That(_database.ListLength("list"), Is.EqualTo(2));
        }

        [Test]
        public void CanPushNewItemToLeftOfList()
        {
            _database.ListLeftPush("list", "1");
            _database.ListLeftPush("list", "2");

            _database.ListRange("list").ToStringArray().Should().ContainInOrder("2", "1");
        }

        [Test]
        public void CanPushNewItemToRightOfList()
        {
            _database.ListLeftPush("list", "1");
            _database.ListRightPush("list", "2");

            _database.ListRange("list").ToStringArray().Should().ContainInOrder("1", "2");
        }

        [Test]
        public void CanGetElementOfListByIndex()
        {
            _database.ListRightPush("list", "1");
            _database.ListRightPush("list", "2");
            _database.ListRightPush("list", "3");

            _database.ListGetByIndex("list", 1).Should().Be("2");
        }

        [Test]
        public void CanSetElementOfListByIndex()
        {
            _database.ListRightPush("list", "1");
            _database.ListRightPush("list", "2");
            _database.ListRightPush("list", "3");

            _database.ListSetByIndex("list", 1, "9");

            _database.ListGetByIndex("list", 1).Should().Be("9");
        }

        [Test]
        public void CanRemoveAllElementsFromListWithMatchingValue()
        {
            _database.ListRightPush("list", "1");
            _database.ListRightPush("list", "2");
            _database.ListRightPush("list", "3");
            _database.ListRightPush("list", "2");

            _database.ListRemove("list", "2");

            _database.ListRange("list").ToStringArray().Should().ContainInOrder("1", "3");

        }

        [Test]
        public void CanRemoveLimitedNumberOfElementFromListByValue()
        {
            _database.ListRightPush("list", "2");
            _database.ListRightPush("list", "1");
            _database.ListRightPush("list", "2");
            _database.ListRightPush("list", "3");
            _database.ListRightPush("list", "2");

            _database.ListRemove("list", "2", 2);

            _database.ListRange("list").ToStringArray().Should().ContainInOrder("1", "3", "2");
        }

        [Test]
        public void CanInsertElementIntoListBeforeFirstInstanceOfSpecifiedExistingValue()
        {
            _database.ListRightPush("list", "1");
            _database.ListRightPush("list", "2");
            _database.ListRightPush("list", "3");
            _database.ListRightPush("list", "2");

            _database.ListInsertBefore("list", "2", "9");

            _database.ListRange("list").ToStringArray().Should().ContainInOrder("1", "9", "2", "3", "2");
        }


        [Test]
        public void CanInsertElementIntoListAfterFirstInstanceOfSpecifiedExistingValue()
        {
            _database.ListRightPush("list", "1");
            _database.ListRightPush("list", "2");
            _database.ListRightPush("list", "3");
            _database.ListRightPush("list", "2");

            _database.ListInsertAfter("list", "2", "9");

            _database.ListRange("list").ToStringArray().Should().ContainInOrder("1", "2", "9", "3", "2");
        }

        [Test]
        public void CanPopElementFromLeftOfList()
        {
            _database.ListRightPush("list", "1");
            _database.ListRightPush("list", "2");

            _database.ListLeftPop("list").Should().Be("1");
            _database.ListRange("list").ToStringArray().Should().ContainInOrder("2");
        }

        [Test]
        public void CanPopElementFromRightOfList()
        {
            _database.ListRightPush("list", "1");
            _database.ListRightPush("list", "2");

            _database.ListRightPop("list").Should().Be("2");
            _database.ListRange("list").ToStringArray().Should().ContainInOrder("1");
        }

        [Test]
        public void CanPushValuePoppedFromOneListToAnother()
        {
            _database.ListRightPush("list1", "1");
            _database.ListRightPush("list1", "2");

            _database.ListRightPush("list2", "3");
            _database.ListRightPush("list2", "4");

            _database.ListRightPopLeftPush("list1", "list2");

            _database.ListRange("list1").ToStringArray().Should().ContainInOrder("1");
            _database.ListRange("list2").ToStringArray().Should().ContainInOrder("2", "3", "4");
        }

        [Test]
        public void CanTrimListToRetainOnlySpecifiedRange()
        {
            _database.ListRightPush("list", "1");
            _database.ListRightPush("list", "2");
            _database.ListRightPush("list", "3");
            _database.ListRightPush("list", "4");
            _database.ListRightPush("list", "5");

            _database.ListTrim("list", 1, 3);

            _database.ListRange("list").ToStringArray().Should().ContainInOrder("2", "3", "4");
        }

        // Blocking operations are not directly implemented by StackExchange.Redis
    }
}
