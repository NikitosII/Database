namespace Database.Core.Models
{
    /// <summary>
    /// Points to a specific record on disk: which page and which slot within that page.
    /// </summary>
    public readonly struct RecordLocation : IEquatable<RecordLocation>
    {
        public int PageId { get; }
        public int SlotIndex { get; }

        public RecordLocation(int pageId, int slotIndex)
        {
            PageId = pageId;
            SlotIndex = slotIndex;
        }

        public bool Equals(RecordLocation other) =>
            PageId == other.PageId && SlotIndex == other.SlotIndex;

        public override bool Equals(object? obj) =>
            obj is RecordLocation other && Equals(other);

        public override int GetHashCode() =>
            HashCode.Combine(PageId, SlotIndex);

        public override string ToString() => $"Page={PageId}, Slot={SlotIndex}";

        public static bool operator ==(RecordLocation left, RecordLocation right) => left.Equals(right);
        public static bool operator !=(RecordLocation left, RecordLocation right) => !left.Equals(right);
    }
}
