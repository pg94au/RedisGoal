using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentAssertions;
using NUnit.Framework;
using StackExchange.Redis;

namespace RedisTests
{
    public class GeospatialTests : RedisTestFixture
    {
        [Test]
        public void CanAddPositionAndCheckIfItExists()
        {
            Database.GeoAdd("geo", new GeoEntry(12d, 34d, "home"));

            var position = Database.GeoPosition("geo", "home");

            position.HasValue.Should().BeTrue();
            position.Value.Longitude.Should().BeApproximately(12d, 0.0001d);
            position.Value.Latitude.Should().BeApproximately(34d, 0.0001d);
        }

        [Test]
        public void CanAddMoreThanOnePositionWithSameCoordinates()
        {
            Database.GeoAdd("geo", new GeoEntry(12d, 23d, "home"));
            Database.GeoAdd("geo", new GeoEntry(34d, 45d, "work"));

            var home = Database.GeoPosition("geo", "home");
            var work = Database.GeoPosition("geo", "work");

            home.HasValue.Should().BeTrue();
            home.Value.Longitude.Should().BeApproximately(12d, 0.0001d);
            home.Value.Latitude.Should().BeApproximately(23d, 0.0001d);

            work.HasValue.Should().BeTrue();
            work.Value.Longitude.Should().BeApproximately(34d, 0.0001d);
            work.Value.Latitude.Should().BeApproximately(45d, 0.0001d);
        }

        [Test]
        public void CanMoveExistingPosition()
        {
            Database.GeoAdd("geo", new GeoEntry(12d, 23d, "home"));
            Database.GeoAdd("geo", new GeoEntry(34d, 45d, "home"));

            var home = Database.GeoPosition("geo", "home");

            home.HasValue.Should().BeTrue();
            home.Value.Longitude.Should().BeApproximately(34d, 0.0001d);
            home.Value.Latitude.Should().BeApproximately(45d, 0.0001d);
        }

        [Test]
        public void CanRemovePosition()
        {
            Database.GeoAdd("geo", new GeoEntry(12d, 23d, "home"));
            Database.GeoRemove("geo", "home");

            var home = Database.GeoPosition("geo", "home");

            home.HasValue.Should().BeFalse();
        }

        [Test]
        public void CanGetDistanceBetweenTwoPositions()
        {
            Database.GeoAdd("geo", new GeoEntry(12d, 23d, "home"));
            Database.GeoAdd("geo", new GeoEntry(34d, 45d, "work"));

            var distance = Database.GeoDistance("geo", "home", "work", GeoUnit.Kilometers);

            distance.Should().BeGreaterOrEqualTo(0).And.BeLessOrEqualTo(10000);
        }

        [Test]
        public void CanLocateAllPositionsWithinRadiusOfSpecifiedPosition()
        {
            Database.GeoAdd("geo", new GeoEntry(0d, 0d, "p1"));
            Database.GeoAdd("geo", new GeoEntry(10d, 10d, "p2"));
            Database.GeoAdd("geo", new GeoEntry(20d, 20d, "p3"));
            Database.GeoAdd("geo", new GeoEntry(-5d, 5d, "p4"));

            var withinRadius = Database.GeoRadius("geo", "p1", 2000, GeoUnit.Kilometers);

            withinRadius.Should().HaveCount(3);
            withinRadius.Select(grr => grr.Member.ToString()).Should().BeEquivalentTo("p1", "p2", "p4");
        }

        [Test]
        public void CanLocateAllPositionsWithinRadiusOfSpecifiedCoordinates()
        {
            Database.GeoAdd("geo", new GeoEntry(0d, 0d, "p1"));
            Database.GeoAdd("geo", new GeoEntry(10d, 10d, "p2"));
            Database.GeoAdd("geo", new GeoEntry(20d, 20d, "p3"));
            Database.GeoAdd("geo", new GeoEntry(-5d, 5d, "p4"));

            var withinRadius = Database.GeoRadius("geo", 0d, 0d, 2000, GeoUnit.Kilometers);

            withinRadius.Should().HaveCount(3);
            withinRadius.Select(grr => grr.Member.ToString()).Should().BeEquivalentTo("p1", "p2", "p4");
        }

        [Test]
        public void CanGetGeoHashOfPosition()
        {
            Database.GeoAdd("geo", new GeoEntry(12d, 23d, "home"));

            var geoHash = Database.GeoHash("geo", "home");

            geoHash.Should().NotBeNullOrEmpty();
        }
    }
}
