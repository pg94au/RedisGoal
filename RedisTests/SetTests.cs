using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentAssertions;
using NUnit.Framework;
using StackExchange.Redis;

namespace RedisTests
{
    public class SetTests : RedisTestFixture
    {
        [Test]
        public void CanAddMemberToNewSet()
        {
            Database.SetAdd("set", "1");

            Database.SetMembers("set").ToStringArray().Should().ContainInOrder("1");
        }

        [Test]
        public void SetsCanContainUniqueMembers()
        {
            Database.SetAdd("set", "1");
            Database.SetAdd("set", "2");

            Database.SetMembers("set").ToStringArray().Should().ContainInOrder("1", "2");
        }

        [Test]
        public void SetsCannotContainDuplicateMembers()
        {
            Database.SetAdd("set", "1");
            Database.SetAdd("set", "1");

            Database.SetMembers("set").ToStringArray().Should().ContainInOrder("1");
        }

        [Test]
        public void CanRemoveMemberFromSet()
        {
            Database.SetAdd("set", "1");
            Database.SetAdd("set", "2");

            Database.SetRemove("set", "2");

            Database.SetMembers("set").ToStringArray().Should().ContainInOrder("1");
        }

        [Test]
        public void CanCheckIfSetHasMember()
        {
            Database.SetAdd("set", "1");
            Database.SetAdd("set", "2");

            Database.SetContains("set", "2").Should().BeTrue();
        }

        [Test]
        public void CanDetermineSizeOfSet()
        {
            Database.SetAdd("set", "1");
            Database.SetAdd("set", "2");

            Database.SetLength("set").Should().Be(2);
        }

        [Test]
        public void CanPopRandomMemberFromSet()
        {
            Database.SetAdd("set", "1");
            Database.SetAdd("set", "2");
            Database.SetAdd("set", "3");

            string popped = Database.SetPop("set").ToString();

            popped.Should().BeOneOf("1", "2", "3");
            Database.SetMembers("set").ToStringArray().Should().NotContain(popped);
        }

        [Test]
        public void CanPopSpecifiedNumberOfRandomMembersFromSet()
        {
            Database.SetAdd("set", "1");
            Database.SetAdd("set", "2");
            Database.SetAdd("set", "3");

            var popped = Database.SetPop("set", 2).ToStringArray();

            popped.Should().BeSubsetOf(new[] {"1", "2", "3"});
            Database.SetMembers("set").ToStringArray().Should().NotContain(popped);
        }

        [Test]
        public void CanGetDifferenceBetweenTwoSets()
        {
            Database.SetAdd("set1", "1");
            Database.SetAdd("set1", "2");
            Database.SetAdd("set1", "3");
            Database.SetAdd("set1", "4");

            Database.SetAdd("set2", "1");
            Database.SetAdd("set2", "3");

            var difference = Database.SetCombine(SetOperation.Difference, "set1", "set2").ToStringArray();

            difference.Should().BeEquivalentTo("2", "4");
        }

        [Test]
        public void CanGetDifferenceBetweenOneSetAndMultipleOthers()
        {
            Database.SetAdd("set1", "1");
            Database.SetAdd("set1", "2");
            Database.SetAdd("set1", "3");
            Database.SetAdd("set1", "4");

            Database.SetAdd("set2", "1");
            Database.SetAdd("set2", "3");

            Database.SetAdd("set3", "4");

            var difference = Database.SetCombine(SetOperation.Difference, new RedisKey[] { "set1", "set2", "set3"}).ToStringArray();

            difference.Should().BeEquivalentTo("2");
        }

        [Test]
        public void CanStoreDifferenceBetweenSetsIntoNewSet()
        {
            Database.SetAdd("set1", "1");
            Database.SetAdd("set1", "2");
            Database.SetAdd("set1", "3");
            Database.SetAdd("set1", "4");

            Database.SetAdd("set2", "1");
            Database.SetAdd("set2", "3");

            var set3Length = Database.SetCombineAndStore(SetOperation.Difference, "set3", "set1", "set2");

            set3Length.Should().Be(2);
            Database.SetMembers("set3").ToStringArray().Should().BeEquivalentTo("2", "4");
        }

        [Test]
        public void CanGetIntersectionOfTwoSets()
        {
            Database.SetAdd("set1", "1");
            Database.SetAdd("set1", "2");
            Database.SetAdd("set1", "3");
            Database.SetAdd("set1", "4");

            Database.SetAdd("set2", "1");
            Database.SetAdd("set2", "3");

            var intersection = Database.SetCombine(SetOperation.Intersect, "set1", "set2").ToStringArray();

            intersection.Should().BeEquivalentTo("1", "3");
        }

        [Test]
        public void CanGetIntersectionOfMultipleSets()
        {
            Database.SetAdd("set1", "1");
            Database.SetAdd("set1", "2");
            Database.SetAdd("set1", "3");
            Database.SetAdd("set1", "4");

            Database.SetAdd("set2", "1");
            Database.SetAdd("set2", "3");

            Database.SetAdd("set3", "3");
            Database.SetAdd("set3", "4");

            var intersection = Database.SetCombine(SetOperation.Intersect, new RedisKey[] { "set1", "set2", "set3" }).ToStringArray();

            intersection.Should().BeEquivalentTo("3");
        }

        [Test]
        public void CanStoreIntersectionOfSetsIntoNewSet()
        {
            Database.SetAdd("set1", "1");
            Database.SetAdd("set1", "2");
            Database.SetAdd("set1", "3");
            Database.SetAdd("set1", "4");

            Database.SetAdd("set2", "1");
            Database.SetAdd("set2", "3");

            var set3Length = Database.SetCombineAndStore(SetOperation.Intersect, "set3", "set1", "set2");

            set3Length.Should().Be(2);
            Database.SetMembers("set3").ToStringArray().Should().BeEquivalentTo("1", "3");
        }

        [Test]
        public void CanGetUnionOfTwoSets()
        {
            Database.SetAdd("set1", "1");
            Database.SetAdd("set1", "2");
            Database.SetAdd("set1", "3");

            Database.SetAdd("set2", "1");
            Database.SetAdd("set2", "4");

            var union = Database.SetCombine(SetOperation.Union, "set1", "set2").ToStringArray();

            union.Should().BeEquivalentTo("1", "2", "3", "4");
        }

        [Test]
        public void CanGetUnionOfMultipleSets()
        {
            Database.SetAdd("set1", "1");
            Database.SetAdd("set1", "2");

            Database.SetAdd("set2", "3");
            Database.SetAdd("set2", "5");

            Database.SetAdd("set3", "4");
            Database.SetAdd("set3", "5");

            var union = Database.SetCombine(SetOperation.Union, new RedisKey[] { "set1", "set2", "set3" }).ToStringArray();

            union.Should().BeEquivalentTo("1", "2", "3", "4", "5");
        }

        [Test]
        public void CanStoreUnionOfSetsIntoNewSet()
        {
            Database.SetAdd("set1", "1");
            Database.SetAdd("set1", "2");

            Database.SetAdd("set2", "1");
            Database.SetAdd("set2", "3");

            var set3Length = Database.SetCombineAndStore(SetOperation.Union, "set3", "set1", "set2");

            set3Length.Should().Be(3);
            Database.SetMembers("set3").ToStringArray().Should().BeEquivalentTo("1", "2", "3");
        }

        [Test]
        public void CanMoveAMemberFromOneSetToAnother()
        {
            Database.SetAdd("set1", "1");
            Database.SetAdd("set1", "2");

            Database.SetMove("set1", "set2", "2");

            Database.SetContains("set1", "2").Should().BeFalse();
            Database.SetContains("set2", "2").Should().BeTrue();
        }

        [Test]
        public void CanEnumerateOverAllSetMembers()
        {
            Database.SetAdd("set", "1");
            Database.SetAdd("set", "2");
            Database.SetAdd("set", "3");
            Database.SetAdd("set", "4");

            var results = Database.SetScan("set");

            results.Should().BeEquivalentTo(new RedisValue[] {"1", "2", "3", "4"});
        }

        [Test]
        public void CanEnumerateSetMembersStartingAtPosition()
        {
            Database.SetAdd("set", "1");
            Database.SetAdd("set", "2");
            Database.SetAdd("set", "3");
            Database.SetAdd("set", "4");

            // This behaves rather unexpectedly.  Page size does not matter from the perspective
            // of the API, because the cursor is handled internally so that you can just keep
            // iterating on the result and additional pages will be automatically fetched to keep
            // results coming in.  It also appears that the page offset has nothing to do with
            // the defined page size.  It is treated as the index to start at.
            var results = Database.SetScan("set", default(RedisValue), pageOffset: 2);

            results.Should().BeEquivalentTo(new RedisValue[] { "3", "4" });
        }
    }
}
