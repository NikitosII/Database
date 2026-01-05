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

        /// <summary>
            /// 1 Вставить пару ключ-значение в индекс
            /// 2 Удалить значение из индекса по ключу
            /// 3 Найти все значения по ключу
            /// 4 Найти значения в диапазоне ключей
            /// 5 Получить минимальный ключ
            /// 6 Получить максимальный ключ
        /// </summary>

        System.Threading.Tasks.ValueTask InsertAsync(TKey key, TValue value);

        System.Threading.Tasks.ValueTask<bool> DeleteAsync(TKey key, TValue value);

        System.Collections.Generic.IAsyncEnumerable<TValue> FindAsync(TKey key);

        System.Collections.Generic.IAsyncEnumerable<TValue> FindRangeAsync(TKey min, TKey max, bool inclusiveMin = true, bool inclusiveMax = true);

        System.Threading.Tasks.ValueTask<TKey> GetMinKeyAsync();

        System.Threading.Tasks.ValueTask<TKey> GetMaxKeyAsync();
    }
}