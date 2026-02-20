namespace Database.Core.Interfaces
{
    /// <summary>
    /// Non-generic base interface for all indexes.
    /// </summary>
    public interface IIndex
    {
        string FieldName { get; }
        Type KeyType { get; }
        Type ValueType { get; }
    }

    /// <summary>
    /// Generic interface for typed index operations (insert, delete, find, range scan).
    /// </summary>
    public interface IIndex<TKey, TValue> : IIndex
    {
        void Insert(TKey key, TValue value);
        bool Delete(TKey key, TValue value);
        IEnumerable<TValue> Find(TKey key);
        IEnumerable<TValue> FindRange(TKey min, TKey max, bool inclusiveMin = true, bool inclusiveMax = true);
        TKey GetMinKey();
        TKey GetMaxKey();
    }
}
