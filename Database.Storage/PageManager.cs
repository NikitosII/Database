using Database.Core.Interfaces;
using Database.Core.Models;

namespace Database.Storage
{
    /// <summary>
    /// Manages a file of fixed-size slotted pages.
    /// Implements IRecordStorage — the bridge between raw pages and
    /// the higher-level record insert/read/delete operations.
    /// </summary>
    public class PageManager : IRecordStorage
    {
        private readonly FileStream _file;
        private readonly int _pageSize;

        public int PageCount => (int)(_file.Length / _pageSize);

        public PageManager(string path, int pageSize = 4096)
        {
            _pageSize = pageSize;
            _file = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        }

        // ----- Low-level page I/O -----

        private SlottedPage ReadPage(int pageId)
        {
            var buffer = new byte[_pageSize];
            _file.Seek((long)pageId * _pageSize, SeekOrigin.Begin);
            int read = _file.Read(buffer, 0, _pageSize);
            if (read < _pageSize)
                throw new InvalidOperationException($"Incomplete page read: got {read} bytes");
            return new SlottedPage(buffer);
        }

        private void WritePage(int pageId, SlottedPage page)
        {
            _file.Seek((long)pageId * _pageSize, SeekOrigin.Begin);
            _file.Write(page.GetRawData(), 0, _pageSize);
            _file.Flush();
        }

        private int AllocatePage()
        {
            int newPageId = PageCount;
            var empty = new SlottedPage(_pageSize);
            WritePage(newPageId, empty);
            return newPageId;
        }

        // ----- IRecordStorage -----

        /// <summary>
        /// Inserts a variable-length record into the first page with enough space.
        /// If no page has space, allocates a new one.
        /// </summary>
        public RecordLocation Insert(byte[] data)
        {
            if (data.Length + 4 > _pageSize - 8) // SlotEntrySize(4) + HeaderSize(8) minimum
                throw new ArgumentException($"Record too large ({data.Length} bytes) for page size {_pageSize}");

            // Try to find an existing page with enough free space
            for (int pageId = 0; pageId < PageCount; pageId++)
            {
                var page = ReadPage(pageId);
                int slotIndex = page.Insert(data);
                if (slotIndex >= 0)
                {
                    WritePage(pageId, page);
                    return new RecordLocation(pageId, slotIndex);
                }
            }

            // No page had space — allocate new one
            int newPageId = AllocatePage();
            var newPage = ReadPage(newPageId);
            int slot = newPage.Insert(data);
            WritePage(newPageId, newPage);
            return new RecordLocation(newPageId, slot);
        }

        /// <summary>
        /// Reads a record from disk given its page and slot location.
        /// </summary>
        public byte[] Read(RecordLocation location)
        {
            var page = ReadPage(location.PageId);
            var data = page.Get(location.SlotIndex);
            return data ?? throw new KeyNotFoundException(
                $"Record not found at {location} (deleted or invalid)");
        }

        /// <summary>
        /// Deletes the old record and inserts the new data, returning the new location.
        /// </summary>
        public void Update(RecordLocation oldLocation, byte[] newData, out RecordLocation newLocation)
        {
            Delete(oldLocation);
            newLocation = Insert(newData);
        }

        /// <summary>
        /// Marks a record slot as deleted (tombstone).
        /// </summary>
        public void Delete(RecordLocation location)
        {
            var page = ReadPage(location.PageId);
            page.Delete(location.SlotIndex);
            WritePage(location.PageId, page);
        }

        /// <summary>
        /// Scans every page and every live slot, returning all records.
        /// Used at startup to rebuild in-memory indexes.
        /// </summary>
        public IEnumerable<(RecordLocation Location, byte[] Data)> ScanAll()
        {
            for (int pageId = 0; pageId < PageCount; pageId++)
            {
                var page = ReadPage(pageId);
                foreach (var (slotIndex, data) in page.EnumerateRecords())
                {
                    yield return (new RecordLocation(pageId, slotIndex), data);
                }
            }
        }

        public void Dispose()
        {
            _file.Dispose();
        }
    }
}
