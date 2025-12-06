using Database.Core.Interfaces;
using Microsoft.Extensions.Logging;
using OwnDatabase.Indexing.Interfaces;
using System.Collections.Frozen;

namespace Database.Query.Query
{
    public class QueryEngine<T>
    {
        private readonly IRecordStorage<T> _record;
        private readonly FrozenDictionary<string, IIndex> _indexes;
        private readonly ILogger<QueryEngine<T>> _logger;

        public QueryEngine(IRecordStorage<T> record, IEnumerable<IIndex> indexes, ILogger<QueryEngine<T>>? logger = null)
        {
            _record = record ?? throw new ArgumentNullException(nameof(record));
            _indexes = indexes?.ToFrozenDictionary(x=>x.FieldName) ?? throw new ArgumentNullException(nameof(indexes));
            _logger = logger;
        }

        // Выполняет запрос с использованием индексов
        public async IAsyncEnumerable<T> ExecuteQuery()
        {

        }

        // Пытается использовать индекс для выполнения запроса
        private bool TryUseIndex()
        {

        }

        // Выполняет запрос равенства с использованием индекса
        private async IAsyncEnumerable<T> ExecuteEqualityIndexQuery()
        {

        }

        // Generic версия запроса равенства
        private static async IAsyncEnumerable<TResult> ExecuteEqualityIndexQueryGeneric<TKey, TValue, TResult>()
        {

        }


    }
}
