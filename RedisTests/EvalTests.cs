using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentAssertions;
using StackExchange.Redis;

namespace RedisTests
{
    public class EvalTests : RedisTestFixture
    {
        [Test]
        public void ScriptCanReturnSingleValue()
        {
            var script = "return 'foo'";

            var result = Database.ScriptEvaluate(script, new RedisKey[0], new RedisValue[0]);

            result.IsNull.Should().BeFalse();
            result.Type.Should().Be(ResultType.BulkString);
            result.ToString().Should().Be("foo");
        }

        [Test]
        public void ScriptCanReturnArray()
        {
            var script = "return {'one', 'two', 'three'}";

            var result = Database.ScriptEvaluate(script, new RedisKey[0], new RedisValue[0]);

            result.IsNull.Should().BeFalse();
            result.Type.Should().Be(ResultType.MultiBulk);

            var results = (RedisResult[])result;
            results.Select(r => r.Type).Should().AllBeEquivalentTo(ResultType.BulkString);
            results[0].ToString().Should().Be("one");
            results[1].ToString().Should().Be("two");
            results[2].ToString().Should().Be("three");
        }

        [Test]
        public void ScriptCanReceiveKeysAndValues()
        {
            var script = "return {KEYS[1], KEYS[2], ARGV[1], ARGV[2]}";

            var result = Database.ScriptEvaluate(script, new RedisKey[] { "key1", "key2" }, new RedisValue[] { "value1", "value2" });

            result.IsNull.Should().BeFalse();
            result.Type.Should().Be(ResultType.MultiBulk);

            var results = (RedisResult[])result;
            results.Select(r => r.Type).Should().AllBeEquivalentTo(ResultType.BulkString);
            results[0].ToString().Should().Be("key1");
            results[1].ToString().Should().Be("key2");
            results[2].ToString().Should().Be("value1");
            results[3].ToString().Should().Be("value2");
        }
    }
}
