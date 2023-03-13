using System.Threading.Tasks;
using FluentAssertions;
using NUnit.Framework;

namespace CallCenter.Tests
{
    [TestFixture]
    public class CallCenterTests : RedisTestFixture
    {
        [Test]
        public async Task CanAddAttendant()
        {
            var callCenter = new CallCenter(RedisContainer.GetConnectionString());
            await callCenter.FlushAsync();

            await callCenter.AddAttendantAsync("Bob", new [] {"English", "French"}, new [] {"Plumbing", "Fishing"});

            (await callCenter.GetAttendantsAsync()).Should().Contain("Bob");
        }

        [Test]
        public async Task CanRemoveAttendant()
        {
            var callCenter = new CallCenter(RedisContainer.GetConnectionString());
            await callCenter.FlushAsync();

            await callCenter.AddAttendantAsync("Bob", new[] { "English", "French" }, new[] { "Plumbing", "Fishing" });

            await callCenter.RemoveAttendantAsync("Bob");

            (await callCenter.GetAttendantsAsync()).Should().BeEmpty();
        }

        [Test]
        public async Task CanAssignSuitableAttendant()
        {
            var callCenter = new CallCenter(RedisContainer.GetConnectionString());
            await callCenter.FlushAsync();

            await callCenter.AddAttendantAsync("Bob", new[] { "English", "French" }, new[] { "Plumbing", "Fishing" });

            var chosen = await callCenter.AssignAttendantAsync(new[] {"French"}, new[] {"Fishing"});

            chosen.Should().Be("Bob");

            (await callCenter.GetBusyAttendantsAsync()).Should().Contain("Bob");
        }

        [Test]
        public async Task WhenNoSuitableAttendantNoneAssigned()
        {
            var callCenter = new CallCenter(RedisContainer.GetConnectionString());
            await callCenter.FlushAsync();

            await callCenter.AddAttendantAsync("Bob", new[] { "English", "French" }, new[] { "Plumbing", "Fishing" });

            var chosen = await callCenter.AssignAttendantAsync(new[] { "French" }, new[] { "Knitting" });

            chosen.Should().BeNull();

            (await callCenter.GetBusyAttendantsAsync()).Should().BeEmpty();
        }

        [Test]
        public async Task BusyAttendantsAreNotSuitableForAssignment()
        {
            var callCenter = new CallCenter(RedisContainer.GetConnectionString());
            await callCenter.FlushAsync();

            await callCenter.AddAttendantAsync("Bob", new[] { "English", "French" }, new[] { "Plumbing", "Fishing" });

            var busy = await callCenter.AssignAttendantAsync(new[] { "French" }, new[] { "Fishing" });

            await callCenter.AddAttendantAsync("Fred", new[] { "English", "French" }, new[] { "Plumbing", "Fishing" });

            var chosen = await callCenter.AssignAttendantAsync(new[] { "French" }, new[] { "Fishing" });
            chosen.Should().Be("Fred");

            (await callCenter.GetBusyAttendantsAsync()).Should().Contain("Bob", "Fred");
        }

        [Test]
        public async Task WhenOnlySuitableCandidatesAreBusyNoneIsSelected()
        {
            var callCenter = new CallCenter(RedisContainer.GetConnectionString());
            await callCenter.FlushAsync();

            await callCenter.AddAttendantAsync("Bob", new[] { "English", "French" }, new[] { "Plumbing", "Fishing" });

            var busy = await callCenter.AssignAttendantAsync(new[] { "French" }, new[] { "Fishing" });

            var chosen = await callCenter.AssignAttendantAsync(new[] { "French" }, new[] { "Fishing" });
            chosen.Should().BeNull();

            (await callCenter.GetBusyAttendantsAsync()).Should().Contain("Bob");
        }

        [Test]
        public async Task CanSetBusyAttendantAvailable()
        {
            var callCenter = new CallCenter(RedisContainer.GetConnectionString());
            await callCenter.FlushAsync();

            await callCenter.AddAttendantAsync("Bob", new[] { "English", "French" }, new[] { "Plumbing", "Fishing" });

            var busy = await callCenter.AssignAttendantAsync(new[] { "French" }, new[] { "Fishing" });

            (await callCenter.SetAttendantAvailableAsync(busy)).Should().BeTrue();

            (await callCenter.GetBusyAttendantsAsync()).Should().BeEmpty();
        }

        [Test]
        public async Task MarkingAvailableAttendantAvailableHasNoEffect()
        {
            var callCenter = new CallCenter(RedisContainer.GetConnectionString());
            await callCenter.FlushAsync();

            await callCenter.AddAttendantAsync("Bob", new[] { "English", "French" }, new[] { "Plumbing", "Fishing" });

            (await callCenter.SetAttendantAvailableAsync("Bob")).Should().BeFalse();

            (await callCenter.GetBusyAttendantsAsync()).Should().BeEmpty();
        }

        [Test]
        public async Task BusyAttendantsSetAvailableCanBeSelectedAgain()
        {
            var callCenter = new CallCenter(RedisContainer.GetConnectionString());
            await callCenter.FlushAsync();

            await callCenter.AddAttendantAsync("Bob", new[] { "English", "French" }, new[] { "Plumbing", "Fishing" });

            await callCenter.AssignAttendantAsync(new[] { "French" }, new[] { "Fishing" });
            await callCenter.SetAttendantAvailableAsync("Bob");

            var busy = await callCenter.AssignAttendantAsync(new[] { "French" }, new[] { "Fishing" });
            busy.Should().Be("Bob");

            (await callCenter.GetBusyAttendantsAsync()).Should().Contain("Bob");
        }

        [Test]
        public async Task RemovedAttendantsCannotBeSelected()
        {
            var callCenter = new CallCenter(RedisContainer.GetConnectionString());
            await callCenter.FlushAsync();

            await callCenter.AddAttendantAsync("Bob", new[] { "English", "French" }, new[] { "Plumbing", "Fishing" });
            await callCenter.RemoveAttendantAsync("Bob");

            var chosen = await callCenter.AssignAttendantAsync(new[] { "French" }, new[] { "Fishing" });

            chosen.Should().BeNull();
        }
    }
}
