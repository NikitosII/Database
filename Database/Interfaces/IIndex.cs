using System.Collections.Generic;

namespace OwnDatabase.Indexing.Interfaces
{
    // интерфейс для всех индексов
    public interface IIndex
    {
        string FieldName { get; }
        System.Type KeyType { get; }
        System.Type ValueType { get; }
    }


    // Generic интерфейс для индексов с поддержкой типизированных операций
    public interface IIndex<TKey, TValue> : IIndex
    {
        /// Вставить пару ключ-значение в индекс
        System.Threading.Tasks.ValueTask InsertAsync(TKey key, TValue value);

        /// Удалить значение из индекса по ключу
        System.Threading.Tasks.ValueTask<bool> DeleteAsync(TKey key, TValue value);

        /// Найти все значения по ключу
        System.Collections.Generic.IAsyncEnumerable<TValue> FindAsync(TKey key);

        /// Найти значения в диапазоне ключей
        System.Collections.Generic.IAsyncEnumerable<TValue> FindRangeAsync(TKey min, TKey max, bool inclusiveMin = true, bool inclusiveMax = true);

        /// Получить минимальный ключ
        System.Threading.Tasks.ValueTask<TKey> GetMinKeyAsync();

        /// Получить максимальный ключ
        System.Threading.Tasks.ValueTask<TKey> GetMaxKeyAsync();
    }
}