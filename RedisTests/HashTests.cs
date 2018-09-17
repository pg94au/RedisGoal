using System;
using System.Collections.Generic;
using System.Text;
using FluentAssertions;
using NUnit.Framework;
using StackExchange.Redis;

namespace RedisTests
{
    public class HashTests : RedisTestFixture
    {
        [Test]
        public void CanSetFieldInHashAndRetrieveIt()
        {
            Database.HashSet("hash", "foo", "bar");

            Database.HashGet("hash", "foo").ToString().Should().Be("bar");
        }

        [Test]
        public void CanSetMultipleFieldsInHashAtOnceAndRetrieveThem()
        {
            Database.HashSet("hash", new[] {new HashEntry("key1", "value1"), new HashEntry("key2", "value2")});

            var hash = Database.HashGetAll("hash").ToStringDictionary();

            hash.Keys.Should().BeEquivalentTo("key1", "key2");
            hash["key1"].Should().Be("value1");
            hash["key2"].Should().Be("value2");
        }

        [Test]
        public void CanConditionallySetValueForFieldThatDoNotAlreadyExist()
        {
            Database.HashSet("hash", "key1", "value1");

            Database.HashSet("hash", "key1", "XXX", When.NotExists);
            Database.HashSet("hash", "key2", "value2", When.NotExists);

            Database.HashGet("hash", "key1").Should().Be("value1");
            Database.HashGet("hash", "key2").Should().Be("value2");
        }

        [Test]
        public void CanReplaceValueForExistingField()
        {
            Database.HashSet("hash", "key", "value1");

            Database.HashSet("hash", "key", "value2", When.Always);

            Database.HashGet("hash", "key").Should().Be("value2");
        }

        [Test]
        public void CanCheckIfFieldExists()
        {
            Database.HashSet("hash", "key", "value");

            Database.HashExists("hash", "key").Should().BeTrue();
            Database.HashExists("hash", "XXX").Should().BeFalse();
        }

        [Test]
        public void CanDeleteFieldFromHash()
        {
            Database.HashSet("hash", "key", "value");

            Database.HashDelete("hash", "key");

            Database.HashExists("hash", "key").Should().BeFalse();
        }

        [Test]
        public void CanIncrementValueOfFieldBySpecifiedValue()
        {
            Database.HashSet("hash", "long", 100);
            Database.HashSet("hash", "double", 1.00d);

            Database.HashIncrement("hash", "long", 23);
            Database.HashIncrement("hash", "double", 0.23d);

            Database.HashGet("hash", "long").Should().Be(123);
            Database.HashGet("hash", "double").Should().Be(1.23d);
        }

        [Test]
        public void CanGetAllKeysFromHash()
        {
            Database.HashSet("hash", "key1", "value1");
            Database.HashSet("hash", "key2", "value2");

            Database.HashKeys("hash").ToStringArray().Should().BeEquivalentTo("key1", "key2");
        }

        [Test]
        public void CanGetAllValuesFromHash()
        {
            Database.HashSet("hash", "key1", "value1");
            Database.HashSet("hash", "key2", "value2");

            Database.HashValues("hash").ToStringArray().Should().BeEquivalentTo("value1", "value2");
        }

        [Test]
        public void CanGetNumberOfFieldsInHash()
        {
            Database.HashSet("hash", "key1", "value1");
            Database.HashSet("hash", "key2", "value2");

            Database.HashLength("hash").Should().Be(2);
        }

        [Test]
        public void CanGetValuesOfMultipleHashFields()
        {
            Database.HashSet("hash", "key1", "value1");
            Database.HashSet("hash", "key2", "value2");
            Database.HashSet("hash", "key3", "value3");

            Database.HashGet("hash", new RedisValue[] {"key1", "key3"}).ToStringArray().Should().BeEquivalentTo("value1", "value3");
        }

        [Test]
        public void CanScanThroughFieldsStartingFromOffset()
        {
            Database.HashSet("hash", "key1", "value1");
            Database.HashSet("hash", "key2", "value2");
            Database.HashSet("hash", "key3", "value3");
            Database.HashSet("hash", "key4", "value4");

            // This behaves rather unexpectedly.  Page size does not matter from the perspective
            // of the API, because the cursor is handled internally so that you can just keep
            // iterating on the result and additional pages will be automatically fetched to keep
            // results coming in.  It also appears that the page offset has nothing to do with
            // the defined page size.  It is treated as the index to start at.
            var results = Database.HashScan("hash", default(RedisValue), pageOffset: 2);

            results.Should().BeEquivalentTo(new[] {new HashEntry("key3", "value3"), new HashEntry("key4", "value4")});
        }

        [Test]
        public void CanScanThroughFilteredFieldsStartingFromOffset()
        {
            Database.HashSet("hash", "abc", "1");
            Database.HashSet("hash", "abcd", "2");
            Database.HashSet("hash", "xyz", "3");
            Database.HashSet("hash", "abcdefg", "4");

            var results = Database.HashScan("hash", "abc*");

            results.Should().BeEquivalentTo(new[] {new HashEntry("abc", "1"), new HashEntry("abcd", "2"), new HashEntry("abcdefg", "4")});
        }
    }
}
