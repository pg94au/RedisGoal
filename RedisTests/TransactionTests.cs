using System.Threading.Tasks;
using FluentAssertions;
using NUnit.Framework;
using StackExchange.Redis;

namespace RedisTests;

[TestFixture]
public class TransactionTests : RedisTestFixture
{
    [Test]
    public async Task CanExecuteMultipleCommandsInATransactionOriginal()
    {
        var transaction = Database.CreateTransaction();

        var task1 = transaction.StringSetAsync("foo1", "bar1");
        var task2 = transaction.StringSetAsync("foo2", "bar2");

        var committed = await transaction.ExecuteAsync();
        committed.Should().BeTrue();
        task1.IsCompletedSuccessfully.Should().BeTrue();
        task2.IsCompletedSuccessfully.Should().BeTrue();

        (await Database.StringGetAsync("foo1")).Should().Be("bar1");
        (await Database.StringGetAsync("foo2")).Should().Be("bar2");
    }


    [Test]
    public async Task CanExecuteMultipleCommandsInATransaction()
    {
        var transaction = Database.CreateTransaction();

        var task1 = transaction.StringSetAsync("foo1", "bar1");
        var task2 = transaction.StringSetAsync("foo2", "bar2");

        var committed = await transaction.ExecuteAsync();
        committed.Should().BeTrue();
        task1.IsCompletedSuccessfully.Should().BeTrue();
        task2.IsCompletedSuccessfully.Should().BeTrue();

        (await Database.StringGetAsync("foo1")).Should().Be("bar1");
        (await Database.StringGetAsync("foo2")).Should().Be("bar2");
    }

    [Test]
    public async Task TransactionExecutesWhenConditionIsSatisfied()
    {
        await Database.StringSetAsync("foo1", "bar1");
        await Database.StringSetAsync("foo2", "bar2");

        var transaction = Database.CreateTransaction();

        transaction.AddCondition(Condition.StringEqual("foo1", "bar1"));

        var task1 = transaction.StringSetAsync("foo1", "baz1");
        var task2 = transaction.StringSetAsync("foo2", "baz2");

        var committed = await transaction.ExecuteAsync();
        committed.Should().BeTrue();
        task1.IsCompletedSuccessfully.Should().BeTrue();
        task2.IsCompletedSuccessfully.Should().BeTrue();

        (await Database.StringGetAsync("foo1")).Should().Be("baz1");
        (await Database.StringGetAsync("foo2")).Should().Be("baz2");
    }

    [Test]
    public async Task TransactionDoesNotExecuteWhenConditionIsNotSatisfied()
    {
        await Database.StringSetAsync("foo1", "bar1");
        await Database.StringSetAsync("foo2", "bar2");

        var transaction = Database.CreateTransaction();

        transaction.AddCondition(Condition.StringEqual("foo1", "blah"));

        var task1 = transaction.StringSetAsync("foo1", "baz3");
        var task2 = transaction.StringSetAsync("foo2", "baz4");

        var committed = await transaction.ExecuteAsync();
        committed.Should().BeFalse();
        task1.IsCompleted.Should().BeTrue();
        task1.IsCompletedSuccessfully.Should().BeFalse();
        task2.IsCompleted.Should().BeTrue();
        task2.IsCompletedSuccessfully.Should().BeFalse();

        (await Database.StringGetAsync("foo1")).Should().Be("bar1");
        (await Database.StringGetAsync("foo2")).Should().Be("bar2");
    }

    [Test]
    public async Task CanRetrieveResultsFromWithinTransaction()
    {
        Database.StringSet("foo1", "bar1");
        Database.StringSet("foo2", "bar2");

        var transaction = Database.CreateTransaction();

        var task = transaction.StringSetAsync("foo1", "baz1");
        var getTask = transaction.StringGetAsync("foo1");

        var committed = await transaction.ExecuteAsync();
        committed.Should().BeTrue();
        task.IsCompletedSuccessfully.Should().BeTrue();

        var result = await getTask;

        result.Should().Be("baz1");
    }
}