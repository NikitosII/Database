using Xunit;
using OwnDatabase;
using Database.Core.Models;

namespace Database.Tests;

public class CowDatabaseTests : IDisposable
{
    private readonly string _dbPath;
    private CowDatabase _db;

    public CowDatabaseTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_cows_{Guid.NewGuid()}.odb");
        _db = new CowDatabase(_dbPath);
    }

    public void Dispose()
    {
        _db.Dispose();
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);
    }

    private CowModel MakeCow(string breed = "Holstein", int age = 3, string name = "Bessie", byte[]? dna = null)
    {
        return new CowModel
        {
            Id = Guid.NewGuid(),
            Breed = breed,
            Age = age,
            Name = name,
            DnaData = dna ?? new byte[] { 0xAB, 0xCD }
        };
    }

    [Fact]
    public void Insert_And_Find_ById()
    {
        var cow = MakeCow();
        _db.Insert(cow);

        var found = _db.Find(cow.Id);

        Assert.NotNull(found);
        Assert.Equal(cow.Id, found.Id);
        Assert.Equal("Holstein", found.Breed);
        Assert.Equal(3, found.Age);
        Assert.Equal("Bessie", found.Name);
    }

    [Fact]
    public void Find_NonExistent_ReturnsNull()
    {
        Assert.Null(_db.Find(Guid.NewGuid()));
    }

    [Fact]
    public void FindBy_BreedAndAge()
    {
        _db.Insert(MakeCow("Angus", 2, "Ann"));
        _db.Insert(MakeCow("Angus", 2, "Bob"));
        _db.Insert(MakeCow("Angus", 5, "Carl"));
        _db.Insert(MakeCow("Jersey", 2, "Dee"));

        var results = _db.FindBy("Angus", 2).ToList();

        Assert.Equal(2, results.Count);
        Assert.All(results, c => Assert.Equal("Angus", c.Breed));
        Assert.All(results, c => Assert.Equal(2, c.Age));
    }

    [Fact]
    public void Delete_RemovesCow()
    {
        var cow = MakeCow();
        _db.Insert(cow);

        _db.Delete(cow);

        Assert.Null(_db.Find(cow.Id));
    }

    [Fact]
    public void Update_ChangesCow()
    {
        var cow = MakeCow("Holstein", 3, "Bessie");
        _db.Insert(cow);

        cow.Age = 4;
        cow.Name = "BessieUpdated";
        _db.Update(cow);

        var found = _db.Find(cow.Id);
        Assert.NotNull(found);
        Assert.Equal(4, found.Age);
        Assert.Equal("BessieUpdated", found.Name);
    }

    [Fact]
    public void Update_ChangesSecondaryIndex()
    {
        var cow = MakeCow("Holstein", 3, "Bessie");
        _db.Insert(cow);

        // Change breed/age
        cow.Breed = "Angus";
        cow.Age = 5;
        _db.Update(cow);

        // Old key should not find anything
        Assert.Empty(_db.FindBy("Holstein", 3));
        // New key should find the cow
        var results = _db.FindBy("Angus", 5).ToList();
        Assert.Single(results);
        Assert.Equal(cow.Id, results[0].Id);
    }

    [Fact]
    public void Insert_DuplicateId_Throws()
    {
        var cow = MakeCow();
        _db.Insert(cow);

        Assert.Throws<InvalidOperationException>(() => _db.Insert(cow));
    }

    [Fact]
    public void Persistence_DataSurvivesReopen()
    {
        var cow = MakeCow("Hereford", 6, "Persistent");
        _db.Insert(cow);
        var savedId = cow.Id;

        // Close and reopen
        _db.Dispose();
        _db = new CowDatabase(_dbPath);

        var found = _db.Find(savedId);
        Assert.NotNull(found);
        Assert.Equal("Hereford", found.Breed);
        Assert.Equal(6, found.Age);
        Assert.Equal("Persistent", found.Name);
    }

    [Fact]
    public void Persistence_FindBy_WorksAfterReopen()
    {
        _db.Insert(MakeCow("Angus", 2, "A"));
        _db.Insert(MakeCow("Angus", 2, "B"));

        _db.Dispose();
        _db = new CowDatabase(_dbPath);

        var results = _db.FindBy("Angus", 2).ToList();
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public void Insert_LargeDnaData_Works()
    {
        var dna = new byte[2000];
        Random.Shared.NextBytes(dna);
        var cow = MakeCow(dna: dna);
        _db.Insert(cow);

        var found = _db.Find(cow.Id);
        Assert.NotNull(found);
        Assert.Equal(dna, found.DnaData);
    }

    [Fact]
    public void Insert_ManyCows_AllRetrievable()
    {
        var ids = new List<Guid>();
        for (int i = 0; i < 100; i++)
        {
            var cow = MakeCow("Breed" + (i % 5), i % 10, "Cow" + i);
            _db.Insert(cow);
            ids.Add(cow.Id);
        }

        foreach (var id in ids)
            Assert.NotNull(_db.Find(id));
    }
}
