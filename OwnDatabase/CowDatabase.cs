using Database.Core.Interfaces;
using Database.Core.Models;
using Database.Indexing;
using Database.Serialization;
using Database.Storage;

namespace OwnDatabase
{
    /// <summary>
    /// The main CowDatabase implementation.
    ///
    /// Architecture:
    ///   ┌───────────────────────────────────────────────┐
    ///   │              CowDatabase                      │
    ///   │  ┌─────────┐  ┌──────────┐  ┌─────────────┐  │
    ///   │  │ Primary  │  │Secondary │  │   Record    │  │
    ///   │  │ Index    │  │  Index   │  │   Storage   │  │
    ///   │  │(Guid→Loc)│  │(B,A→Loc) │  │ (PageMgr)  │  │
    ///   │  └─────────┘  └──────────┘  └──────┬──────┘  │
    ///   │                                    │         │
    ///   │                            ┌───────▼───────┐ │
    ///   │                            │  Data File    │ │
    ///   │                            │ (slotted pgs) │ │
    ///   │                            └───────────────┘ │
    ///   └───────────────────────────────────────────────┘
    ///
    /// - Records are stored on disk using slotted pages (variable-length binary).
    /// - Two B-tree indexes are kept in memory:
    ///     1. Primary: Guid Id → RecordLocation   (for Find)
    ///     2. Secondary: (Breed, Age) → RecordLocation (for FindBy)
    /// - On startup, all pages are scanned and indexes are rebuilt.
    /// </summary>
    public class CowDatabase : ICowDatabase
    {
        private readonly PageManager _storage;
        private readonly BTree<Guid, RecordLocation> _primaryIndex;
        private readonly BTree<BreedAgeKey, RecordLocation> _secondaryIndex;
        private bool _disposed;

        public CowDatabase(string databasePath, int pageSize = 4096)
        {
            _storage = new PageManager(databasePath, pageSize);

            // Create in-memory B-tree indexes
            var primaryNodeManager = new InMemoryNodeManager<Guid, RecordLocation>();
            _primaryIndex = new BTree<Guid, RecordLocation>("Id", primaryNodeManager, degree: 4);

            var secondaryNodeManager = new InMemoryNodeManager<BreedAgeKey, RecordLocation>();
            _secondaryIndex = new BTree<BreedAgeKey, RecordLocation>("Breed_Age", secondaryNodeManager, degree: 4);

            // Rebuild indexes from existing data on disk
            RebuildIndexes();
        }

        /// <summary>
        /// Scans all pages and re-inserts every live record into both indexes.
        /// Called once at startup.
        /// </summary>
        private void RebuildIndexes()
        {
            foreach (var (location, data) in _storage.ScanAll())
            {
                var cow = CowSerializer.Deserialize(data);
                _primaryIndex.Insert(cow.Id, location);
                _secondaryIndex.Insert(new BreedAgeKey(cow.Breed, cow.Age), location);
            }
        }

        // ----- ICowDatabase -----

        public void Insert(CowModel cow)
        {
            ThrowIfDisposed();
            if (cow.Id == Guid.Empty)
                cow.Id = Guid.NewGuid();

            // Check for duplicate Id
            if (_primaryIndex.Find(cow.Id).Any())
                throw new InvalidOperationException($"A cow with Id {cow.Id} already exists");

            var data = CowSerializer.Serialize(cow);
            var location = _storage.Insert(data);

            _primaryIndex.Insert(cow.Id, location);
            _secondaryIndex.Insert(new BreedAgeKey(cow.Breed, cow.Age), location);
        }

        public CowModel? Find(Guid id)
        {
            ThrowIfDisposed();
            var locations = _primaryIndex.Find(id).ToList();
            if (locations.Count == 0)
                return null;

            var data = _storage.Read(locations[0]);
            return CowSerializer.Deserialize(data);
        }

        public IEnumerable<CowModel> FindBy(string breed, int age)
        {
            ThrowIfDisposed();
            var key = new BreedAgeKey(breed, age);
            var locations = _secondaryIndex.Find(key);

            foreach (var location in locations)
            {
                var data = _storage.Read(location);
                yield return CowSerializer.Deserialize(data);
            }
        }

        public void Update(CowModel cow)
        {
            ThrowIfDisposed();

            // Find the old record location
            var oldLocations = _primaryIndex.Find(cow.Id).ToList();
            if (oldLocations.Count == 0)
                throw new KeyNotFoundException($"No cow found with Id {cow.Id}");
            var oldLocation = oldLocations[0];

            // Read old record to get old breed/age for secondary index removal
            var oldData = _storage.Read(oldLocation);
            var oldCow = CowSerializer.Deserialize(oldData);

            // Serialize updated cow and write
            var newData = CowSerializer.Serialize(cow);
            _storage.Update(oldLocation, newData, out var newLocation);

            // Update primary index: remove old, insert new
            _primaryIndex.Delete(cow.Id, oldLocation);
            _primaryIndex.Insert(cow.Id, newLocation);

            // Update secondary index: remove old key, insert new key
            _secondaryIndex.Delete(new BreedAgeKey(oldCow.Breed, oldCow.Age), oldLocation);
            _secondaryIndex.Insert(new BreedAgeKey(cow.Breed, cow.Age), newLocation);
        }

        public void Delete(CowModel cow)
        {
            ThrowIfDisposed();

            var locations = _primaryIndex.Find(cow.Id).ToList();
            if (locations.Count == 0)
                throw new KeyNotFoundException($"No cow found with Id {cow.Id}");
            var location = locations[0];

            // Read the record so we know the breed/age for secondary index
            var data = _storage.Read(location);
            var existing = CowSerializer.Deserialize(data);

            _storage.Delete(location);
            _primaryIndex.Delete(cow.Id, location);
            _secondaryIndex.Delete(new BreedAgeKey(existing.Breed, existing.Age), location);
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(CowDatabase));
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _disposed = true;
                _storage.Dispose();
            }
        }
    }
}
