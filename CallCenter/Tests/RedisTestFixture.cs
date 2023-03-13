using NUnit.Framework;
using StackExchange.Redis;
using System.Threading.Tasks;
using Testcontainers.Redis;

namespace CallCenter.Tests;

public class RedisTestFixture
{
    protected readonly RedisContainer RedisContainer = new RedisBuilder().Build();
    protected ConnectionMultiplexer ConnectionMultiplexer;
    protected IDatabase Database;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        await RedisContainer.StartAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await RedisContainer.DisposeAsync();
    }

    [SetUp]
    public async Task SetUp()
    {
        // Flush database before each test runs.
        var adminConnectionString = $"{RedisContainer.GetConnectionString()},allowAdmin=true";
        await using var connection = await ConnectionMultiplexer.ConnectAsync(adminConnectionString);
        await connection.GetServer(RedisContainer.GetConnectionString()).FlushAllDatabasesAsync();

        ConnectionMultiplexer = await ConnectionMultiplexer.ConnectAsync(RedisContainer.GetConnectionString());
        Database = ConnectionMultiplexer.GetDatabase();
    }
}