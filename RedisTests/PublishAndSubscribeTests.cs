using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using FluentAssertions;
using NUnit.Framework;
using StackExchange.Redis;

namespace RedisTests
{
    public class PublishAndSubscribeTests : RedisTestFixture
    {
        [Test]
        public void CanSubscribeToChannelAndReceiveMessagePublishedToIt()
        {
            var subscriber = ConnectionMultiplexer.GetSubscriber();

            RedisChannel receivedChannel = default(RedisChannel);
            RedisValue receivedValue = default(RedisValue);
            var receivedEvent = new ManualResetEvent(false);

            subscriber.Subscribe("channel", (channel, value) =>
            {
                receivedChannel = channel;
                receivedValue = value;
                receivedEvent.Set();
            });

            Database.Publish("channel", "Hello");

            receivedEvent.WaitOne(TimeSpan.FromSeconds(5)).Should().BeTrue();
            receivedChannel.Should().Be("channel");
            receivedValue.Should().Be("Hello");
        }

        [Test]
        public void IfNoSubscriberExistsWhenMessageIsPublishedThenMessageIsLost()
        {
            Database.Publish("channel", "Hello");

            var subscriber = ConnectionMultiplexer.GetSubscriber();

            var receivedEvent = new ManualResetEvent(false);

            subscriber.Subscribe("channel", (channel, value) => { receivedEvent.Set(); });

            receivedEvent.WaitOne(TimeSpan.FromSeconds(1)).Should().BeFalse();
        }

        [Test]
        public void WhenMultipleSubscribersExistTheyEachReceivePublishedMessage()
        {
            var subscriber1 = ConnectionMultiplexer.GetSubscriber();
            var subscriber2 = ConnectionMultiplexer.GetSubscriber();

            RedisChannel receivedChannel1 = default(RedisChannel);
            RedisChannel receivedChannel2 = default(RedisChannel);
            RedisValue receivedValue1 = default(RedisValue);
            RedisValue receivedValue2 = default(RedisValue);
            var receivedEvent1 = new ManualResetEvent(false);
            var receivedEvent2 = new ManualResetEvent(false);

            subscriber1.Subscribe("channel", (channel, value) =>
            {
                receivedChannel1 = channel;
                receivedValue1 = value;
                receivedEvent1.Set();
            });

            subscriber2.Subscribe("channel", (channel, value) =>
            {
                receivedChannel2 = channel;
                receivedValue2 = value;
                receivedEvent2.Set();
            });

            Database.Publish("channel", "Hello");

            WaitHandle.WaitAll(new[] {receivedEvent1, receivedEvent2}, TimeSpan.FromSeconds(5))
                .Should()
                .BeTrue();
            receivedChannel1.Should().Be("channel");
            receivedChannel2.Should().Be("channel");
            receivedValue1.Should().Be("Hello");
            receivedValue2.Should().Be("Hello");
        }

        [Test]
        public void CanSubscribeToMultipleChannelsFromOneSubscriber()
        {
            var subscriber = ConnectionMultiplexer.GetSubscriber();

            var receivedEvent1 = new ManualResetEvent(false);
            subscriber.Subscribe("channel1", (channel, value) => { receivedEvent1.Set(); });

            var receivedEvent2 = new ManualResetEvent(false);
            subscriber.Subscribe("channel2", (channel, value) => { receivedEvent2.Set(); });

            Database.Publish("channel1", "Hello");
            receivedEvent1.WaitOne(TimeSpan.FromSeconds(5)).Should().BeTrue();

            Database.Publish("channel2", "Hello");
            receivedEvent2.WaitOne(TimeSpan.FromSeconds(5)).Should().BeTrue();
        }

        [Test]
        public void CanUnsubscribeFromSingleChannel()
        {
            var subscriber = ConnectionMultiplexer.GetSubscriber();

            var receivedEvent1 = new ManualResetEvent(false);
            subscriber.Subscribe("channel1", (channel, value) => { receivedEvent1.Set(); });

            var receivedEvent2 = new ManualResetEvent(false);
            subscriber.Subscribe("channel2", (channel, value) => { receivedEvent2.Set(); });

            subscriber.Unsubscribe("channel1");

            Database.Publish("channel1", "Hello");
            Database.Publish("channel2", "Hello");

            receivedEvent1.WaitOne(TimeSpan.FromSeconds(1)).Should().BeFalse();
            receivedEvent2.WaitOne(TimeSpan.FromSeconds(1)).Should().BeTrue();
        }

        [Test]
        public void CanUnsubscribeFromAllChannels()
        {
            var subscriber = ConnectionMultiplexer.GetSubscriber();

            var receivedEvent1 = new ManualResetEvent(false);
            subscriber.Subscribe("channel1", (channel, value) => { receivedEvent1.Set(); });

            var receivedEvent2 = new ManualResetEvent(false);
            subscriber.Subscribe("channel2", (channel, value) => { receivedEvent2.Set(); });

            subscriber.UnsubscribeAll();

            Database.Publish("channel1", "Hello");
            Database.Publish("channel2", "Hello");

            WaitHandle.WaitAny(new[] {receivedEvent1, receivedEvent2}, TimeSpan.FromSeconds(1))
                .Should()
                .Be(WaitHandle.WaitTimeout);
        }

        [Test]
        public void CanSubscribeToChannelsByPattern()
        {
            var subscriber = ConnectionMultiplexer.GetSubscriber();

            var receivedEvent = new AutoResetEvent(false);
            subscriber.Subscribe("channel*", (channel, value) => { receivedEvent.Set(); });

            Database.Publish("channel1", "Hello");
            receivedEvent.WaitOne(TimeSpan.FromSeconds(1)).Should().BeTrue();

            Database.Publish("channel2", "Hello");
            receivedEvent.WaitOne(TimeSpan.FromSeconds(1)).Should().BeTrue();

            Database.Publish("xxx", "Hello");
            receivedEvent.WaitOne(TimeSpan.FromSeconds(1)).Should().BeFalse();
        }
    }
}
