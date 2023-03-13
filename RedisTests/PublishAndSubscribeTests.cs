using System;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using NUnit.Framework;
using StackExchange.Redis;

namespace RedisTests;

[TestFixture]
public class PublishAndSubscribeTests : RedisTestFixture
{
    [Test]
    public async Task CanSubscribeToChannelAndReceiveMessagePublishedToIt()
    {
        var subscriber = ConnectionMultiplexer.GetSubscriber();

        RedisChannel receivedChannel = default;
        RedisValue receivedValue = default;
        var receivedEvent = new ManualResetEvent(false);

        await subscriber.SubscribeAsync("channel", (channel, value) =>
        {
            receivedChannel = channel;
            receivedValue = value;
            receivedEvent.Set();
        });

        await Database.PublishAsync("channel", "Hello");

        receivedEvent.WaitOne(TimeSpan.FromSeconds(5)).Should().BeTrue();
        receivedChannel.Should().Be("channel");
        receivedValue.Should().Be("Hello");
    }

    [Test]
    public async Task IfNoSubscriberExistsWhenMessageIsPublishedThenMessageIsLost()
    {
        await Database.PublishAsync("channel", "Hello");

        var subscriber = ConnectionMultiplexer.GetSubscriber();

        var receivedEvent = new ManualResetEvent(false);

        await subscriber.SubscribeAsync("channel", (channel, value) => { receivedEvent.Set(); });

        receivedEvent.WaitOne(TimeSpan.FromSeconds(1)).Should().BeFalse();
    }

    [Test]
    public async Task WhenMultipleSubscribersExistTheyEachReceivePublishedMessage()
    {
        var subscriber1 = ConnectionMultiplexer.GetSubscriber();
        var subscriber2 = ConnectionMultiplexer.GetSubscriber();

        RedisChannel receivedChannel1 = default;
        RedisChannel receivedChannel2 = default;
        RedisValue receivedValue1 = default;
        RedisValue receivedValue2 = default;
        var receivedEvent1 = new ManualResetEvent(false);
        var receivedEvent2 = new ManualResetEvent(false);

        await subscriber1.SubscribeAsync("channel", (channel, value) =>
        {
            receivedChannel1 = channel;
            receivedValue1 = value;
            receivedEvent1.Set();
        });

        await subscriber2.SubscribeAsync("channel", (channel, value) =>
        {
            receivedChannel2 = channel;
            receivedValue2 = value;
            receivedEvent2.Set();
        });

        await Database.PublishAsync("channel", "Hello");

        WaitHandle.WaitAll(new WaitHandle[] {receivedEvent1, receivedEvent2}, TimeSpan.FromSeconds(5))
            .Should()
            .BeTrue();
        receivedChannel1.Should().Be("channel");
        receivedChannel2.Should().Be("channel");
        receivedValue1.Should().Be("Hello");
        receivedValue2.Should().Be("Hello");
    }

    [Test]
    public async Task CanSubscribeToMultipleChannelsFromOneSubscriber()
    {
        var subscriber = ConnectionMultiplexer.GetSubscriber();

        var receivedEvent1 = new ManualResetEvent(false);
        await subscriber.SubscribeAsync("channel1", (channel, value) => { receivedEvent1.Set(); });

        var receivedEvent2 = new ManualResetEvent(false);
        await subscriber.SubscribeAsync("channel2", (channel, value) => { receivedEvent2.Set(); });

        await Database.PublishAsync("channel1", "Hello");
        receivedEvent1.WaitOne(TimeSpan.FromSeconds(5)).Should().BeTrue();

        await Database.PublishAsync("channel2", "Hello");
        receivedEvent2.WaitOne(TimeSpan.FromSeconds(5)).Should().BeTrue();
    }

    [Test]
    public async Task CanUnsubscribeFromSingleChannel()
    {
        var subscriber = ConnectionMultiplexer.GetSubscriber();

        var receivedEvent1 = new ManualResetEvent(false);
        await subscriber.SubscribeAsync("channel1", (channel, value) => { receivedEvent1.Set(); });

        var receivedEvent2 = new ManualResetEvent(false);
        await subscriber.SubscribeAsync("channel2", (channel, value) => { receivedEvent2.Set(); });

        await subscriber.UnsubscribeAsync("channel1");

        await Database.PublishAsync("channel1", "Hello");
        await Database.PublishAsync("channel2", "Hello");

        receivedEvent1.WaitOne(TimeSpan.FromSeconds(1)).Should().BeFalse();
        receivedEvent2.WaitOne(TimeSpan.FromSeconds(1)).Should().BeTrue();
    }

    [Test]
    public async Task CanUnsubscribeFromAllChannels()
    {
        var subscriber = ConnectionMultiplexer.GetSubscriber();

        var receivedEvent1 = new ManualResetEvent(false);
        await subscriber.SubscribeAsync("channel1", (channel, value) => { receivedEvent1.Set(); });

        var receivedEvent2 = new ManualResetEvent(false);
        await subscriber.SubscribeAsync("channel2", (channel, value) => { receivedEvent2.Set(); });

        await subscriber.UnsubscribeAllAsync();

        await Database.PublishAsync("channel1", "Hello");
        await Database.PublishAsync("channel2", "Hello");

        WaitHandle.WaitAny(new WaitHandle[] {receivedEvent1, receivedEvent2}, TimeSpan.FromSeconds(1))
            .Should()
            .Be(WaitHandle.WaitTimeout);
    }

    [Test]
    public async Task CanSubscribeToChannelsByPattern()
    {
        var subscriber = ConnectionMultiplexer.GetSubscriber();

        var receivedEvent = new AutoResetEvent(false);
        await subscriber.SubscribeAsync("channel*", (channel, value) => { receivedEvent.Set(); });

        await Database.PublishAsync("channel1", "Hello");
        receivedEvent.WaitOne(TimeSpan.FromSeconds(1)).Should().BeTrue();

        await Database.PublishAsync("channel2", "Hello");
        receivedEvent.WaitOne(TimeSpan.FromSeconds(1)).Should().BeTrue();

        await Database.PublishAsync("xxx", "Hello");
        receivedEvent.WaitOne(TimeSpan.FromSeconds(1)).Should().BeFalse();
    }
}