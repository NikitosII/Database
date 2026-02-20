using Xunit;
using Database.Core.Models;
using Database.Serialization;

namespace Database.Tests;

public class CowSerializerTests
{
    [Fact]
    public void Serialize_Deserialize_RoundTrip()
    {
        var original = new CowModel
        {
            Id = Guid.NewGuid(),
            Breed = "Holstein",
            Age = 5,
            Name = "Bessie",
            DnaData = new byte[] { 0xCA, 0xFE, 0xBA, 0xBE }
        };

        var bytes = CowSerializer.Serialize(original);
        var restored = CowSerializer.Deserialize(bytes);

        Assert.Equal(original.Id, restored.Id);
        Assert.Equal(original.Breed, restored.Breed);
        Assert.Equal(original.Age, restored.Age);
        Assert.Equal(original.Name, restored.Name);
        Assert.Equal(original.DnaData, restored.DnaData);
    }

    [Fact]
    public void Serialize_EmptyStrings_Works()
    {
        var cow = new CowModel
        {
            Id = Guid.NewGuid(),
            Breed = "",
            Age = 0,
            Name = "",
            DnaData = Array.Empty<byte>()
        };

        var bytes = CowSerializer.Serialize(cow);
        var restored = CowSerializer.Deserialize(bytes);

        Assert.Equal(cow.Id, restored.Id);
        Assert.Equal("", restored.Breed);
        Assert.Equal("", restored.Name);
        Assert.Empty(restored.DnaData);
    }

    [Fact]
    public void Serialize_LargeDnaData_Works()
    {
        var dna = new byte[10_000];
        Random.Shared.NextBytes(dna);

        var cow = new CowModel
        {
            Id = Guid.NewGuid(),
            Breed = "Angus",
            Age = 3,
            Name = "BigBoy",
            DnaData = dna
        };

        var bytes = CowSerializer.Serialize(cow);
        var restored = CowSerializer.Deserialize(bytes);

        Assert.Equal(dna, restored.DnaData);
    }

    [Fact]
    public void Serialize_UnicodeStrings_Works()
    {
        var cow = new CowModel
        {
            Id = Guid.NewGuid(),
            Breed = "Корова",      // Russian
            Age = 7,
            Name = "牛太郎",       // Japanese
            DnaData = new byte[] { 1 }
        };

        var bytes = CowSerializer.Serialize(cow);
        var restored = CowSerializer.Deserialize(bytes);

        Assert.Equal("Корова", restored.Breed);
        Assert.Equal("牛太郎", restored.Name);
    }
}
