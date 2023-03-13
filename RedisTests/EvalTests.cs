using System;
using NUnit.Framework;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using StackExchange.Redis;

namespace RedisTests;

[TestFixture]
public class EvalTests : RedisTestFixture
{
    [Test]
    public async Task ScriptCanReturnSingleValue()
    {
        var script = "return 'foo'";

        var result = await Database.ScriptEvaluateAsync(script, Array.Empty<RedisKey>(), Array.Empty<RedisValue>());

        result.IsNull.Should().BeFalse();
        result.Type.Should().Be(ResultType.BulkString);
        result.ToString().Should().Be("foo");
    }

    [Test]
    public async Task ScriptCanReturnArray()
    {
        var script = "return {'one', 'two', 'three'}";

        var result = await Database.ScriptEvaluateAsync(script, Array.Empty<RedisKey>(), Array.Empty<RedisValue>());

        result.IsNull.Should().BeFalse();
        result.Type.Should().Be(ResultType.MultiBulk);

        var results = (RedisResult[])result;
        results.Should().NotBeNull();
        results!.Select(r => r.Type).Should().AllBeEquivalentTo(ResultType.BulkString);
        results[0].ToString().Should().Be("one");
        results[1].ToString().Should().Be("two");
        results[2].ToString().Should().Be("three");
    }

    [Test]
    public async Task ScriptCanReceiveKeysAndValues()
    {
        var script = "return {KEYS[1], KEYS[2], ARGV[1], ARGV[2]}";

        var result = await Database.ScriptEvaluateAsync(script, new RedisKey[] { "key1", "key2" }, new RedisValue[] { "value1", "value2" });

        result.IsNull.Should().BeFalse();
        result.Type.Should().Be(ResultType.MultiBulk);

        var results = (RedisResult[])result;
        results.Should().NotBeNull();
        results!.Select(r => r.Type).Should().AllBeEquivalentTo(ResultType.BulkString);
        results[0].ToString().Should().Be("key1");
        results[1].ToString().Should().Be("key2");
        results[2].ToString().Should().Be("value1");
        results[3].ToString().Should().Be("value2");
    }

    [Test]
    public async Task ScriptCanPassAnyNumberOfKeysAndValues()
    {
        var script = @"
                local result = {}
                
                table.insert(result, #KEYS)
                local keys = """"
                for i, key in ipairs(KEYS) do
                    keys = keys .. key
                end
                table.insert(result, keys)
                
                table.insert(result, #ARGV)
                local args = """"
                for i, arg in ipairs(ARGV) do
                    args = args .. arg
                end
                table.insert(result, args)

                return result
            ";

        var result = await Database.ScriptEvaluateAsync(script, new RedisKey[] {"key1", "key2", "key3"}, new RedisValue[] {"value1", "value2"});

        result.IsNull.Should().BeFalse();
        result.Type.Should().Be(ResultType.MultiBulk);

        var results = (RedisResult[])result;
        results.Should().NotBeNull();
        results![0].Type.Should().Be(ResultType.Integer);
        results[0].ToString().Should().Be("3");
        results[1].Type.Should().Be(ResultType.BulkString);
        results[1].ToString().Should().Be("key1key2key3");
        results[2].Type.Should().Be(ResultType.Integer);
        results[2].ToString().Should().Be("2");
        results[3].Type.Should().Be(ResultType.BulkString);
        results[3].ToString().Should().Be("value1value2");
    }

    [Test]
    public async Task ScriptCanPassArrayAsDelimitedList()
    {
        var script = @"
                local args = {}
                for arg in string.gmatch(ARGV[1], ""[^;]+"") do
                    table.insert(args, arg)
                end

                return args
            ";

        var result = await Database.ScriptEvaluateAsync(script, Array.Empty<RedisKey>(), new RedisValue[] {"abc;def;ghi"});

        result.IsNull.Should().BeFalse();
        result.Type.Should().Be(ResultType.MultiBulk);

        var results = (RedisResult[])result;
        results.Should().NotBeNull();
        results!.Select(r => r.Type).Should().AllBeEquivalentTo(ResultType.BulkString);
        results[0].ToString().Should().Be("abc");
        results[1].ToString().Should().Be("def");
        results[2].ToString().Should().Be("ghi");
    }
}