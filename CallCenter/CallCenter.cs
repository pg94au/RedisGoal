using System;
using System.Collections.Generic;
using System.Linq;
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
        public void AddAttendant(string name, IEnumerable<string> languages, IEnumerable<string> skills)
        {
            using (var connectionMultiplexer = ConnectionMultiplexer.Connect("localhost:6379,allowAdmin=true"))
            {
                var database = connectionMultiplexer.GetDatabase();

                // TODO: Cannot add attendant that already exists.
                database.SetAdd("attendants", name);

                foreach (var language in languages)
                {
                    database.SetAdd("languages", language);
                    database.SetAdd($"language_{language}", name);
                }

                foreach (var skill in skills)
                {
                    database.SetAdd("skills", skill);
                    database.SetAdd($"skill_{skill}", name);
                }
            }
        }

        public void RemoveAttendant(string name)
        {
            using (var connectionMultiplexer = ConnectionMultiplexer.Connect("localhost:6379,allowAdmin=true"))
            {
                var database = connectionMultiplexer.GetDatabase();

                // TODO: Cannot remove attendant while busy.
                database.SetRemove("attendants", name);

                var languages = database.SetMembers("languages").Select(rv => rv.ToString());
                foreach (var language in languages)
                {
                    database.SetRemove($"language_{language}", name);
                }

                var skills = database.SetMembers("skills").Select(rv => rv.ToString());
                foreach (var skill in skills)
                {
                    database.SetRemove($"skill_{skill}", name);
                }
            }
        }

        public IEnumerable<string> GetAttendants()
        {
            using (var connectionMultiplexer = ConnectionMultiplexer.Connect("localhost:6379,allowAdmin=true"))
            {
                var database = connectionMultiplexer.GetDatabase();

                return database.SetMembers("attendants").Select(rv => rv.ToString());
            }
        }

        public string AssignAttendant(IEnumerable<string> languages, IEnumerable<string> skills)
        {
            using (var connectionMultiplexer = ConnectionMultiplexer.Connect("localhost:6379,allowAdmin=true"))
            {
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

                var result = database.ScriptEvaluate(script, keys);

                return result.ToString();
            }
        }

        public IEnumerable<string> GetBusyAttendants()
        {
            using (var connectionMultiplexer = ConnectionMultiplexer.Connect("localhost:6379,allowAdmin=true"))
            {
                var database = connectionMultiplexer.GetDatabase();

                return database.SetMembers("busy").Select(rv => rv.ToString());
            }
        }

        public bool SetAttendantAvailable(string name)
        {
            using (var connectionMultiplexer = ConnectionMultiplexer.Connect("localhost:6379,allowAdmin=true"))
            {
                var database = connectionMultiplexer.GetDatabase();

                return database.SetRemove("busy", name);
            }
        }

        public void Flush()
        {
            using (var connectionMultiplexer = ConnectionMultiplexer.Connect("localhost:6379,allowAdmin=true"))
            {
                var server = connectionMultiplexer.GetServer("localhost:6379");
                server.FlushAllDatabases();
            }
        }
    }
}
