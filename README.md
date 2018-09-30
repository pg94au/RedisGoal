# RedisGoal

This repository contains projects related to my personal goal of learning more about Redis.

## RedisTests

C# project containing tests to demonstrate the functionality of Redis offered by the StackExchange.Redis project.


## Observations

### Publish/Subscribe
* Redis pub/sub offers no persistence or delivery guarantees.
  * Suggestions that persistence can be obtained by also storing messages in a list
  * With no guarantee of delivery of published messagss, is periodic polling of the list still required?
  
