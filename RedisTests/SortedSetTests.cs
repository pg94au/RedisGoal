using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentAssertions;
using NUnit.Framework;
using StackExchange.Redis;

namespace RedisTests
{
    public class SortedSetTests : RedisTestFixture
    {
        [Test]
        public void CanAddMemberWithScore()
        {
            Database.SortedSetAdd("set", "value", 1);

            var entries = Database.SortedSetRangeByScoreWithScores("set");

            entries.Should().HaveCount(1);
            entries.First().Element.ToString().Should().Be("value");
            entries.First().Score.Should().Be(1);
        }

        [Test]
        public void AddingExistingMemberUpdatesScore()
        {
            Database.SortedSetAdd("set", "value", 1);
            Database.SortedSetAdd("set", "value", 5);

            var entries = Database.SortedSetRangeByScoreWithScores("set");

            entries.Should().HaveCount(1);
            entries.First().Element.ToString().Should().Be("value");
            entries.First().Score.Should().Be(5);
        }

        [Test]
        public void CanGetCountOfAllMembers()
        {
            Database.SortedSetAdd("set", "value1", 1);
            Database.SortedSetAdd("set", "value2", 2);

            Database.SortedSetLength("set").Should().Be(2);
        }

        [Test]
        public void CanGetCountOfMembersWithinScoreRange()
        {
            Database.SortedSetAdd("set", "value1", 1);
            Database.SortedSetAdd("set", "value3", 3);
            Database.SortedSetAdd("set", "value5", 5);
            Database.SortedSetAdd("set", "value7", 7);

            Database.SortedSetLength("set", 2, 5).Should().Be(2);
            Database.SortedSetLength("set", 3, 5, Exclude.Start).Should().Be(1);
            Database.SortedSetLength("set", 3, 7, Exclude.Stop).Should().Be(2);
            Database.SortedSetLength("set", 3, 7, Exclude.Both).Should().Be(1);
        }

        [Test]
        public void CanGetCountOfMembersWithinValueRange()
        {
            Database.SortedSetAdd("set", "value1", 1);
            Database.SortedSetAdd("set", "value3", 3);
            Database.SortedSetAdd("set", "value5", 5);
            Database.SortedSetAdd("set", "value7", 7);

            Database.SortedSetLengthByValue("set", "value3", "value5").Should().Be(2);
            Database.SortedSetLengthByValue("set", "value3", "value5", Exclude.Start).Should().Be(1);
            Database.SortedSetLengthByValue("set", "value3", "value7", Exclude.Stop).Should().Be(2);
            Database.SortedSetLengthByValue("set", "value3", "value7", Exclude.Both).Should().Be(1);
        }

        [Test]
        public void CanIncrementScoreOfMemberBySpecifiedValue()
        {
            Database.SortedSetAdd("set", "value", 1);

            Database.SortedSetIncrement("set", "value", 1);

            Database.SortedSetRangeByScoreWithScores("set").First().Score.Should().Be(2);
        }

        [Test]
        public void CanRemoveMember()
        {
            Database.SortedSetAdd("set", "value", 1);

            Database.SortedSetRemove("set", "value");

            Database.SortedSetRangeByValue("set").Length.Should().Be(0);
        }

        [Test]
        public void CanRemoveMembersByScoreRange()
        {
            Database.SortedSetAdd("set", "value1", 1);
            Database.SortedSetAdd("set", "value3", 3);
            Database.SortedSetAdd("set", "value5", 5);
            Database.SortedSetAdd("set", "value7", 7);

            var numRemoved = Database.SortedSetRemoveRangeByScore("set", 2, 7);

            numRemoved.Should().Be(3);
            Database.SortedSetRangeByScore("set").Select(rv => rv.ToString())
                .Should().BeEquivalentTo("value1");
        }

        [Test]
        public void CanRemoveMembersByRankRange()
        {
            Database.SortedSetAdd("set", "value1", 1);
            Database.SortedSetAdd("set", "value3", 3);
            Database.SortedSetAdd("set", "value5", 5);
            Database.SortedSetAdd("set", "value7", 7);
            Database.SortedSetAdd("set", "value9", 9);

            var numRemoved = Database.SortedSetRemoveRangeByRank("set", 1, 3);

            numRemoved.Should().Be(3);
            Database.SortedSetRangeByScore("set").Select(rv => rv.ToString())
                .Should().BeEquivalentTo("value1", "value9");
        }

        [Test]
        public void CanRemoveMembersByValueRange()
        {
            Database.SortedSetAdd("set", "value1", 1);
            Database.SortedSetAdd("set", "value3", 3);
            Database.SortedSetAdd("set", "value5", 5);
            Database.SortedSetAdd("set", "value7", 7);
            Database.SortedSetAdd("set", "value9", 9);

            var numRemoved = Database.SortedSetRemoveRangeByValue("set", "value3", "value8");

            numRemoved.Should().Be(3);
            Database.SortedSetRangeByScore("set").Select(rv => rv.ToString())
                .Should().BeEquivalentTo("value1", "value9");
        }

        [Test]
        public void CanDetermineRankOrderOfMemberByScore()
        {
            Database.SortedSetAdd("set", "value1", 1);
            Database.SortedSetAdd("set", "value3", 3);
            Database.SortedSetAdd("set", "value5", 5);
            Database.SortedSetAdd("set", "value7", 7);

            Database.SortedSetRank("set", "value5", Order.Ascending).Should().Be(2);
            Database.SortedSetRank("set", "value5", Order.Descending).Should().Be(1);
        }

        [Test]
        public void CanGetScoreOfMember()
        {
            Database.SortedSetAdd("set", "value1", 1);

            Database.SortedSetScore("set", "value1").Should().Be(1);
        }

        [Test]
        public void CanStoreIntersectionOfTwoSortedSetsIntoNewOne()
        {
            Database.SortedSetAdd("set1", "value1", 1);
            Database.SortedSetAdd("set1", "value3", 3);
            Database.SortedSetAdd("set1", "value5", 5);
            Database.SortedSetAdd("set1", "value7", 7);

            Database.SortedSetAdd("set2", "value3", 13);
            Database.SortedSetAdd("set2", "value5", 15);
            Database.SortedSetAdd("set2", "value7", 17);
            Database.SortedSetAdd("set2", "value9", 19);

            var setSize = Database.SortedSetCombineAndStore(SetOperation.Intersect, "set3", "set1", "set2", Aggregate.Sum);

            setSize.Should().Be(3);
            Database.SortedSetRangeByScore("set3").Select(rv => rv.ToString())
                .Should().BeEquivalentTo("value3", "value5", "value7");
            Database.SortedSetScore("set3", "value3").Should().Be(16);
        }

        [Test]
        public void CanStoreUnionBetweenTwoSetsIntoNewOne()
        {
            Database.SortedSetAdd("set1", "value1", 1);
            Database.SortedSetAdd("set1", "value3", 3);
            Database.SortedSetAdd("set1", "value5", 5);
            Database.SortedSetAdd("set1", "value7", 7);

            Database.SortedSetAdd("set2", "value3", 13);
            Database.SortedSetAdd("set2", "value5", 15);
            Database.SortedSetAdd("set2", "value7", 17);
            Database.SortedSetAdd("set2", "value9", 19);

            var setSize = Database.SortedSetCombineAndStore(SetOperation.Union, "set3", "set1", "set2", Aggregate.Max);

            setSize.Should().Be(5);
            Database.SortedSetRangeByScore("set3").Select(rv => rv.ToString())
                .Should().BeEquivalentTo("value1", "value3", "value5", "value7", "value9");
            Database.SortedSetScore("set3", "value3").Should().Be(13);
        }

        // No support for ZPOPMIN and ZPOPMAX yet (newly introduced in Redis 5.0.0)
    }
}
