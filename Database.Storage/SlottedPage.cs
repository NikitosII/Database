namespace Database.Storage
{
    /// <summary>
    /// Manages a fixed-size byte buffer as a slotted page for variable-length records.
    ///
    /// Page layout (pageSize bytes total):
    /// ┌─────────────────────────────────────────────────┐
    /// │ Header (8 bytes)                                │
    /// │   SlotCount       (2 bytes, ushort)             │
    /// │   FreeSpaceEnd    (2 bytes, ushort)             │
    /// │   Reserved        (4 bytes)                     │
    /// ├─────────────────────────────────────────────────┤
    /// │ Slot directory (grows forward, 4 bytes/slot)    │
    /// │   Slot 0: Offset (2 bytes) | Length (2 bytes)   │
    /// │   Slot 1: ...                                   │
    /// ├─────────────────────────────────────────────────┤
    /// │ Free space                                      │
    /// ├─────────────────────────────────────────────────┤
    /// │ Record data (grows backward from page end)      │
    /// │   Record N ... Record 0                         │
    /// └─────────────────────────────────────────────────┘
    ///
    /// A deleted slot has Offset = 0xFFFF (tombstone).
    /// </summary>
    public class SlottedPage
    {
        private const int HeaderSize = 8;
        private const int SlotEntrySize = 4;         // 2 bytes offset + 2 bytes length
        private const ushort TombstoneOffset = 0xFFFF;

        private readonly byte[] _data;

        public int PageSize => _data.Length;

        public ushort SlotCount
        {
            get => BitConverter.ToUInt16(_data, 0);
            private set => BitConverter.GetBytes(value).CopyTo(_data, 0);
        }

        /// <summary>
        /// Points to the start of the lowest record (records grow downward from page end).
        /// New records are placed starting at FreeSpaceEnd - recordLength.
        /// </summary>
        public ushort FreeSpaceEnd
        {
            get => BitConverter.ToUInt16(_data, 2);
            private set => BitConverter.GetBytes(value).CopyTo(_data, 2);
        }

        public int FreeSpace => FreeSpaceEnd - (HeaderSize + SlotCount * SlotEntrySize);

        /// <summary>
        /// Creates a new empty slotted page.
        /// </summary>
        public SlottedPage(int pageSize)
        {
            _data = new byte[pageSize];
            SlotCount = 0;
            FreeSpaceEnd = (ushort)pageSize;
        }

        /// <summary>
        /// Wraps an existing raw page buffer (e.g. read from disk).
        /// </summary>
        public SlottedPage(byte[] rawData)
        {
            _data = rawData;
        }

        /// <summary>
        /// Returns the underlying byte array (for writing to disk).
        /// </summary>
        public byte[] GetRawData() => _data;

        // ----- Slot directory helpers -----

        private int SlotDirectoryOffset(int slotIndex) => HeaderSize + slotIndex * SlotEntrySize;

        private (ushort Offset, ushort Length) ReadSlot(int slotIndex)
        {
            int pos = SlotDirectoryOffset(slotIndex);
            ushort offset = BitConverter.ToUInt16(_data, pos);
            ushort length = BitConverter.ToUInt16(_data, pos + 2);
            return (offset, length);
        }

        private void WriteSlot(int slotIndex, ushort offset, ushort length)
        {
            int pos = SlotDirectoryOffset(slotIndex);
            BitConverter.GetBytes(offset).CopyTo(_data, pos);
            BitConverter.GetBytes(length).CopyTo(_data, pos + 2);
        }

        private bool IsDeleted(int slotIndex)
        {
            var (offset, _) = ReadSlot(slotIndex);
            return offset == TombstoneOffset;
        }

        // ----- Public operations -----

        /// <summary>
        /// Inserts a record into this page. Returns the slot index, or -1 if not enough space.
        /// </summary>
        public int Insert(byte[] record)
        {
            int needed = record.Length + SlotEntrySize; // space for data + new slot entry
            if (FreeSpace < needed)
                return -1;

            ushort newOffset = (ushort)(FreeSpaceEnd - record.Length);
            Array.Copy(record, 0, _data, newOffset, record.Length);

            int slotIndex = SlotCount;
            SlotCount++;
            FreeSpaceEnd = newOffset;
            WriteSlot(slotIndex, newOffset, (ushort)record.Length);

            return slotIndex;
        }

        /// <summary>
        /// Reads the record at the given slot index. Returns null if the slot is deleted.
        /// </summary>
        public byte[]? Get(int slotIndex)
        {
            if (slotIndex < 0 || slotIndex >= SlotCount)
                return null;
            if (IsDeleted(slotIndex))
                return null;

            var (offset, length) = ReadSlot(slotIndex);
            var result = new byte[length];
            Array.Copy(_data, offset, result, 0, length);
            return result;
        }

        /// <summary>
        /// Marks a slot as deleted (tombstone). Does not reclaim space within the page.
        /// </summary>
        public void Delete(int slotIndex)
        {
            if (slotIndex < 0 || slotIndex >= SlotCount)
                throw new ArgumentOutOfRangeException(nameof(slotIndex));
            WriteSlot(slotIndex, TombstoneOffset, 0);
        }

        /// <summary>
        /// Enumerates all live (non-deleted) slot indices and their data.
        /// </summary>
        public IEnumerable<(int SlotIndex, byte[] Data)> EnumerateRecords()
        {
            for (int i = 0; i < SlotCount; i++)
            {
                if (IsDeleted(i))
                    continue;
                var (offset, length) = ReadSlot(i);
                var data = new byte[length];
                Array.Copy(_data, offset, data, 0, length);
                yield return (i, data);
            }
        }
    }
}
