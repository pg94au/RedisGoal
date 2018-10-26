using System.Threading.Tasks;
using FluentAssertions;
using NUnit.Framework;
using StackExchange.Redis;

namespace RedisTests
{
    public class TransactionTests : RedisTestFixture
    {
        [Test]
        public void CanExecuteMultipleCommandsInATransaction()
        {
            var transaction = Database.CreateTransaction();

            transaction.StringSetAsync("foo1", "bar1");
            transaction.StringSetAsync("foo2", "bar2");

            var committed = transaction.Execute();
            committed.Should().BeTrue();

            Database.StringGet("foo1").Should().Be("bar1");
            Database.StringGet("foo2").Should().Be("bar2");
        }

        [Test]
        public void TransactionExecutesWhenConditionIsSatisfied()
        {
            Database.StringSet("foo1", "bar1");
            Database.StringSet("foo2", "bar2");

            var transaction = Database.CreateTransaction();

            transaction.AddCondition(Condition.StringEqual("foo1", "bar1"));

            transaction.StringSetAsync("foo1", "baz1");
            transaction.StringSetAsync("foo2", "baz2");

            var committed = transaction.Execute();
            committed.Should().BeTrue();

            Database.StringGet("foo1").Should().Be("baz1");
            Database.StringGet("foo2").Should().Be("baz2");
        }

        [Test]
        public void TransactionDoesNotExecuteWhenConditionIsNotSatisfied()
        {
            Database.StringSet("foo1", "bar1");
            Database.StringSet("foo2", "bar2");

            var transaction = Database.CreateTransaction();

            transaction.AddCondition(Condition.StringEqual("foo1", "blah"));

            transaction.StringSetAsync("foo1", "baz3");
            transaction.StringSetAsync("foo2", "baz4");

            var committed = transaction.Execute();
            committed.Should().BeFalse();

            Database.StringGet("foo1").Should().Be("bar1");
            Database.StringGet("foo2").Should().Be("bar2");
        }

        [Test]
        public async Task CanRetrieveResultsFromWithinTransaction()
        {
            Database.StringSet("foo1", "bar1");
            Database.StringSet("foo2", "bar2");

            var transaction = Database.CreateTransaction();

            transaction.StringSetAsync("foo1", "baz1");
            var getTask = transaction.StringGetAsync("foo1");

            var committed = transaction.Execute();
            committed.Should().BeTrue();

            var result = await getTask;

            result.Should().Be("baz1");
        }
    }
}
