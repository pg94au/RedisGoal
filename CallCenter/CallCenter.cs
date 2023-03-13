using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using StackExchange.Redis;

namespace CallCenter
{
    /// <summary>
    /// This class implements (with the aid of Redis) a very simple call center.
    /// The call center has attendants identified by name, each of which can have multiple skills
    /// and speak multiple languages.
    /// Attendants can be added and removed from the call center (as their shifts start and stop).
    /// When an attendant is needed to service a call, only one which both has the required skills
    /// and also speaks the correct language can be selected.
    /// Attendants cannot be selected while they are busy.
    /// </summary>
    public class CallCenter
    {
        private readonly string _redisConnectionString;

        public CallCenter(string redisConnectionString)
        {
            _redisConnectionString = redisConnectionString;
        }

        public async Task AddAttendantAsync(string name, IEnumerable<string> languages, IEnumerable<string> skills)
        {
            await using var connectionMultiplexer = await ConnectionMultiplexer.ConnectAsync(_redisConnectionString);
            var database = connectionMultiplexer.GetDatabase();

            // TODO: Cannot add attendant that already exists.
            await database.SetAddAsync("attendants", name);

            foreach (var language in languages)
            {
                await database.SetAddAsync("languages", language);
                await database.SetAddAsync($"language_{language}", name);
            }

            foreach (var skill in skills)
            {
                await database.SetAddAsync("skills", skill);
                await database.SetAddAsync($"skill_{skill}", name);
            }
        }

        public async Task RemoveAttendantAsync(string name)
        {
            await using var connectionMultiplexer = await ConnectionMultiplexer.ConnectAsync(_redisConnectionString);
            var database = connectionMultiplexer.GetDatabase();

            // TODO: Cannot remove attendant while busy.
            await database.SetRemoveAsync("attendants", name);

            var languages = (await database.SetMembersAsync("languages")).Select(rv => rv.ToString());
            foreach (var language in languages)
            {
                await database.SetAddAsync($"language_{language}", name);
            }

            var skills = (await database.SetMembersAsync("skills")).Select(rv => rv.ToString());
            foreach (var skill in skills)
            {
                await database.SetRemoveAsync($"skill_{skill}", name);
            }
        }

        public async Task<IEnumerable<string>> GetAttendantsAsync()
        {
            await using var connectionMultiplexer = await ConnectionMultiplexer.ConnectAsync(_redisConnectionString);
            var database = connectionMultiplexer.GetDatabase();

            return (await database.SetMembersAsync("attendants")).Select(rv => rv.ToString());
        }

        [ItemCanBeNull]
        public async Task<string> AssignAttendantAsync(IEnumerable<string> languages, IEnumerable<string> skills)
        {
            await using var connectionMultiplexer = await ConnectionMultiplexer.ConnectAsync(_redisConnectionString);
            var database = connectionMultiplexer.GetDatabase();

            var script = @"
                    -- Needed because writes are non-deterministic.
                    redis.replicate_commands()

                    -- Put intersection of all requirements sets into target set.
                    local findCandidates = {}
                    table.insert(findCandidates, 'SINTERSTORE')
                    for i, v in ipairs(KEYS) do
                        table.insert(findCandidates, v)
                    end
                    redis.call(unpack(findCandidates))

                    -- Remove busy candidates.
                    redis.call('SDIFFSTORE', KEYS[1], KEYS[1], 'busy')

                    -- Choose one from the intersection result.
                    local chosen = redis.call('SPOP', KEYS[1], 1)

                    -- Clean up intersection set now that candidate is chosen.
                    redis.call('DEL', KEYS[1])

                    -- If there was a candidate, return it (otherwise no one was suitable).
                    if #chosen == 1 then
                        redis.call('SADD', 'busy', chosen[1])
                        return chosen[1]
                    end
                ";

            var relevantSets = languages.Select(l => $"language_{l}")
                .Concat(skills.Select(s => $"skill_{s}"));
            var keys = new RedisKey[] {Guid.NewGuid().ToString()}
                .Concat(relevantSets.Select(r => (RedisKey) r)).ToArray();

            var result = await database.ScriptEvaluateAsync(script, keys);

            return result.IsNull ? null : result.ToString();
        }

        public async Task<IEnumerable<string>> GetBusyAttendantsAsync()
        {
            await using var connectionMultiplexer = await ConnectionMultiplexer.ConnectAsync(_redisConnectionString);
            var database = connectionMultiplexer.GetDatabase();

            return (await database.SetMembersAsync("busy")).Select(rv => rv.ToString());
        }

        public async Task<bool> SetAttendantAvailableAsync(string name)
        {
            await using var connectionMultiplexer = await ConnectionMultiplexer.ConnectAsync(_redisConnectionString);
            var database = connectionMultiplexer.GetDatabase();

            return await database.SetRemoveAsync("busy", name);
        }

        public async Task FlushAsync()
        {
            await using var connectionMultiplexer = await ConnectionMultiplexer.ConnectAsync($"{_redisConnectionString},allowAdmin=true");
            var server = connectionMultiplexer.GetServer(_redisConnectionString);
            await server.FlushAllDatabasesAsync();
        }
    }
}
