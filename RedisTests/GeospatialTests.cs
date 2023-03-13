using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using NUnit.Framework;
using StackExchange.Redis;

namespace RedisTests;

[TestFixture]
public class GeospatialTests : RedisTestFixture
{
    [Test]
    public async Task CanAddPositionAndCheckIfItExists()
    {
        await Database.GeoAddAsync("geo", new GeoEntry(12d, 34d, "home"));

        var position = await Database.GeoPositionAsync("geo", "home");

        position.HasValue.Should().BeTrue();
        position!.Value.Longitude.Should().BeApproximately(12d, 0.0001d);
        position.Value.Latitude.Should().BeApproximately(34d, 0.0001d);
    }

    [Test]
    public async Task CanAddMoreThanOnePositionWithSameCoordinates()
    {
        await Database.GeoAddAsync("geo", new GeoEntry(12d, 23d, "home"));
        await Database.GeoAddAsync("geo", new GeoEntry(34d, 45d, "work"));

        var home = await Database.GeoPositionAsync("geo", "home");
        var work = await Database.GeoPositionAsync("geo", "work");

        home.HasValue.Should().BeTrue();
        home!.Value.Longitude.Should().BeApproximately(12d, 0.0001d);
        home.Value.Latitude.Should().BeApproximately(23d, 0.0001d);

        work.HasValue.Should().BeTrue();
        work!.Value!.Longitude.Should().BeApproximately(34d, 0.0001d);
        work.Value.Latitude.Should().BeApproximately(45d, 0.0001d);
    }

    [Test]
    public async Task CanMoveExistingPosition()
    {
        await Database.GeoAddAsync("geo", new GeoEntry(12d, 23d, "home"));
        await Database.GeoAddAsync("geo", new GeoEntry(34d, 45d, "home"));

        var home = await Database.GeoPositionAsync("geo", "home");

        home.HasValue.Should().BeTrue();
        home!.Value.Longitude.Should().BeApproximately(34d, 0.0001d);
        home.Value.Latitude.Should().BeApproximately(45d, 0.0001d);
    }

    [Test]
    public async Task CanRemovePosition()
    {
        await Database.GeoAddAsync("geo", new GeoEntry(12d, 23d, "home"));
        await Database.GeoRemoveAsync("geo", "home");

        var home = await Database.GeoPositionAsync("geo", "home");

        home.HasValue.Should().BeFalse();
    }

    [Test]
    public async Task CanGetDistanceBetweenTwoPositions()
    {
        await Database.GeoAddAsync("geo", new GeoEntry(12d, 23d, "home"));
        await Database.GeoAddAsync("geo", new GeoEntry(34d, 45d, "work"));

        var distance = await Database.GeoDistanceAsync("geo", "home", "work", GeoUnit.Kilometers);

        distance.Should().BeGreaterOrEqualTo(0).And.BeLessOrEqualTo(10000);
    }

    [Test]
    public async Task CanLocateAllPositionsWithinRadiusOfSpecifiedPosition()
    {
        await Database.GeoAddAsync("geo", new GeoEntry(0d, 0d, "p1"));
        await Database.GeoAddAsync("geo", new GeoEntry(10d, 10d, "p2"));
        await Database.GeoAddAsync("geo", new GeoEntry(20d, 20d, "p3"));
        await Database.GeoAddAsync("geo", new GeoEntry(-5d, 5d, "p4"));

        var withinRadius = await Database.GeoRadiusAsync("geo", "p1", 2000, GeoUnit.Kilometers);

        withinRadius.Should().HaveCount(3);
        withinRadius.Select(grr => grr.Member.ToString()).Should().BeEquivalentTo("p1", "p2", "p4");
    }

    [Test]
    public async Task CanLocateAllPositionsWithinRadiusOfSpecifiedCoordinates()
    {
        await Database.GeoAddAsync("geo", new GeoEntry(0d, 0d, "p1"));
        await Database.GeoAddAsync("geo", new GeoEntry(10d, 10d, "p2"));
        await Database.GeoAddAsync("geo", new GeoEntry(20d, 20d, "p3"));
        await Database.GeoAddAsync("geo", new GeoEntry(-5d, 5d, "p4"));

        var withinRadius = await Database.GeoRadiusAsync("geo", 0d, 0d, 2000, GeoUnit.Kilometers);

        withinRadius.Should().HaveCount(3);
        withinRadius.Select(grr => grr.Member.ToString()).Should().BeEquivalentTo("p1", "p2", "p4");
    }

    [Test]
    public async Task CanGetGeoHashOfPosition()
    {
        await Database.GeoAddAsync("geo", new GeoEntry(12d, 23d, "home"));

        var geoHash = await Database.GeoHashAsync("geo", "home");

        geoHash.Should().NotBeNullOrEmpty();
    }
}