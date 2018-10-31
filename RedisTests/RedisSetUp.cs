using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Docker.DotNet;
using Docker.DotNet.Models;
using NUnit.Framework;
using StackExchange.Redis;

namespace RedisTests
{
    [SetUpFixture]
    public class RedisSetUp
    {
        private DockerClient _dockerClient;
        private string _redisContainerId;
        public static int Port { get; private set; }

        [OneTimeSetUp]
        public void OneTimeSetUpAttributemeSetUp()
        {
            // Create Docker client instance.
            _dockerClient = new DockerClientConfiguration(new Uri("npipe://./pipe/docker_engine")).CreateClient();

            PullRedisImage();

            FindFreePort();

            _redisContainerId = CreateRedisContainer();

            StartRedisContainer();

            WaitForRedisToBeAvailable();
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            StopRedisContainer();

            _dockerClient.Dispose();
        }

        private void PullRedisImage()
        {
            _dockerClient.Images.CreateImageAsync(
                new ImagesCreateParameters
                {
                    FromImage = "redis",
                    Tag = "5.0-rc-alpine"
                },
                new AuthConfig(),
                new Progress<JSONMessage>()
            ).Wait();
        }

        private void FindFreePort()
        {
            IPEndPoint defaultLoopbackEndpoint = new IPEndPoint(IPAddress.Loopback, port: 0);

            using (var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp))
            {
                socket.Bind(defaultLoopbackEndpoint);
                Port = ((IPEndPoint)socket.LocalEndPoint).Port;
            }
        }

        private string CreateRedisContainer()
        {
            var redisContainerId = _dockerClient.Containers.CreateContainerAsync(
                new CreateContainerParameters(
                    new Config
                    {
                        Image = "redis:5.0-rc-alpine",
                        Tty = false,
                        ExposedPorts = new Dictionary<string, EmptyStruct>
                        {
                            {"6379/tcp", new EmptyStruct()}
                        }
                    }
                )
                {
                    HostConfig = new HostConfig
                    {
                        PortBindings = new Dictionary<string, IList<PortBinding>>
                        {
                            {"6379/tcp", new List<PortBinding> {new PortBinding {HostPort = $"{Port}"}}}
                        },
                        AutoRemove = true
                    }
                }
            ).Result.ID;

            return redisContainerId;
        }

        private void StartRedisContainer()
        {
            var started = _dockerClient.Containers.StartContainerAsync(_redisContainerId, new ContainerStartParameters()).Result;
            if (!started)
            {
                throw new Exception("Redis container failed to start.");
            }
        }

        private void WaitForRedisToBeAvailable()
        {
            DateTime startOfWaiting = DateTime.Now;
            bool redisAvailable = false;
            while (!redisAvailable)
            {
                try
                {
                    using (var connectionMultiplexer = ConnectionMultiplexer.Connect($"localhost:{Port}"))
                    {
                        connectionMultiplexer.GetDatabase();
                        redisAvailable = true;
                    }
                }
                catch (Exception)
                {
                    if (DateTime.Now.Subtract(startOfWaiting) > TimeSpan.FromSeconds(10))
                    {
                        throw new Exception("Timed out waiting for Redis to be accessible.");
                    }
                    Thread.Sleep(TimeSpan.FromSeconds(0.5));
                }
            }
        }

        private void StopRedisContainer()
        {
            _dockerClient.Containers.StopContainerAsync(_redisContainerId, new ContainerStopParameters { WaitBeforeKillSeconds = 10 }).Wait();
        }
    }
}
