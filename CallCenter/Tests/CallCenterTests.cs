using FluentAssertions;
using NUnit.Framework;

namespace CallCenter.Tests
{
    [TestFixture]
    public class CallCenterTests
    {
        [Test]
        public void CanAddAttendant()
        {
            var callCenter = new CallCenter("localhost", RedisSetUp.Port);
            callCenter.Flush();

            callCenter.AddAttendant("Bob", new [] {"English", "French"}, new [] {"Plumbing", "Fishing"});

            callCenter.GetAttendants().Should().Contain("Bob");
        }

        [Test]
        public void CanRemoveAttendant()
        {
            var callCenter = new CallCenter("localhost", RedisSetUp.Port);
            callCenter.Flush();

            callCenter.AddAttendant("Bob", new[] { "English", "French" }, new[] { "Plumbing", "Fishing" });

            callCenter.RemoveAttendant("Bob");

            callCenter.GetAttendants().Should().BeEmpty();
        }

        [Test]
        public void CanAssignSuitableAttendant()
        {
            var callCenter = new CallCenter("localhost", RedisSetUp.Port);
            callCenter.Flush();

            callCenter.AddAttendant("Bob", new[] { "English", "French" }, new[] { "Plumbing", "Fishing" });

            var chosen = callCenter.AssignAttendant(new[] {"French"}, new[] {"Fishing"});

            chosen.Should().Be("Bob");

            callCenter.GetBusyAttendants().Should().Contain("Bob");
        }

        [Test]
        public void WhenNoSuitableAttendantNoneAssigned()
        {
            var callCenter = new CallCenter("localhost", RedisSetUp.Port);
            callCenter.Flush();

            callCenter.AddAttendant("Bob", new[] { "English", "French" }, new[] { "Plumbing", "Fishing" });

            var chosen = callCenter.AssignAttendant(new[] { "French" }, new[] { "Knitting" });

            chosen.Should().BeNull();

            callCenter.GetBusyAttendants().Should().BeEmpty();
        }

        [Test]
        public void BusyAttendantsAreNotSuitableForAssignment()
        {
            var callCenter = new CallCenter("localhost", RedisSetUp.Port);
            callCenter.Flush();

            callCenter.AddAttendant("Bob", new[] { "English", "French" }, new[] { "Plumbing", "Fishing" });

            var busy = callCenter.AssignAttendant(new[] { "French" }, new[] { "Fishing" });

            callCenter.AddAttendant("Fred", new[] { "English", "French" }, new[] { "Plumbing", "Fishing" });

            var chosen = callCenter.AssignAttendant(new[] { "French" }, new[] { "Fishing" });
            chosen.Should().Be("Fred");

            callCenter.GetBusyAttendants().Should().Contain("Bob", "Fred");
        }

        [Test]
        public void WhenOnlySuitableCandidatesAreBusyNoneIsSelected()
        {
            var callCenter = new CallCenter("localhost", RedisSetUp.Port);
            callCenter.Flush();

            callCenter.AddAttendant("Bob", new[] { "English", "French" }, new[] { "Plumbing", "Fishing" });

            var busy = callCenter.AssignAttendant(new[] { "French" }, new[] { "Fishing" });

            var chosen = callCenter.AssignAttendant(new[] { "French" }, new[] { "Fishing" });
            chosen.Should().BeNull();

            callCenter.GetBusyAttendants().Should().Contain("Bob");
        }

        [Test]
        public void CanSetBusyAttendantAvailable()
        {
            var callCenter = new CallCenter("localhost", RedisSetUp.Port);
            callCenter.Flush();

            callCenter.AddAttendant("Bob", new[] { "English", "French" }, new[] { "Plumbing", "Fishing" });

            var busy = callCenter.AssignAttendant(new[] { "French" }, new[] { "Fishing" });

            callCenter.SetAttendantAvailable(busy).Should().BeTrue();

            callCenter.GetBusyAttendants().Should().BeEmpty();
        }

        [Test]
        public void MarkingAvailableAttendantAvailableHasNoEffect()
        {
            var callCenter = new CallCenter("localhost", RedisSetUp.Port);
            callCenter.Flush();

            callCenter.AddAttendant("Bob", new[] { "English", "French" }, new[] { "Plumbing", "Fishing" });

            callCenter.SetAttendantAvailable("Bob").Should().BeFalse();

            callCenter.GetBusyAttendants().Should().BeEmpty();
        }

        [Test]
        public void BusyAttendantsSetAvailableCanBeSelectedAgain()
        {
            var callCenter = new CallCenter("localhost", RedisSetUp.Port);
            callCenter.Flush();

            callCenter.AddAttendant("Bob", new[] { "English", "French" }, new[] { "Plumbing", "Fishing" });

            callCenter.AssignAttendant(new[] { "French" }, new[] { "Fishing" });
            callCenter.SetAttendantAvailable("Bob");

            var busy = callCenter.AssignAttendant(new[] { "French" }, new[] { "Fishing" });
            busy.Should().Be("Bob");

            callCenter.GetBusyAttendants().Should().Contain("Bob");
        }

        [Test]
        public void RemovedAttendantsCannotBeSelected()
        {
            var callCenter = new CallCenter("localhost", RedisSetUp.Port);
            callCenter.Flush();

            callCenter.AddAttendant("Bob", new[] { "English", "French" }, new[] { "Plumbing", "Fishing" });
            callCenter.RemoveAttendant("Bob");

            var chosen = callCenter.AssignAttendant(new[] { "French" }, new[] { "Fishing" });

            chosen.Should().BeNull();
        }
    }
}
