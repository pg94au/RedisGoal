using System.Threading.Tasks;
using FluentAssertions;
using NUnit.Framework;
using StackExchange.Redis;

namespace RedisTests;

[TestFixture]
public class FlushTests : RedisTestFixture
{
    [Test]
    public async Task FlushAllDatabasesRemovesEverythingFromRedis()
    {
        await using var connectionMultiplexer = await ConnectionMultiplexer.ConnectAsync($"{RedisContainer.GetConnectionString()},allowAdmin=true");

        var database = connectionMultiplexer.GetDatabase();
        await database.StringSetAsync("foo", "bar");

        var server = connectionMultiplexer.GetServer(RedisContainer.GetConnectionString());
        await server.FlushAllDatabasesAsync();

        (await database.KeyExistsAsync("foo")).Should().BeFalse();
    }
}