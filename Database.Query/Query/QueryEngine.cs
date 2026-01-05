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
        public async IAsyncEnumerable<T> ExecuteQuery(QueryExpression query)
        {
            if (TryUseIndex(query, out var indexes))
            {
                await foreach (var index in indexes)
                {
                    yield return index;
                }
            }
            else
            {
                // Полное сканирование таблицы
                await foreach (var record in _record.ScanAsync())
                {
                    if (EvaluateException(query, record))
                    {
                        yield return record;
                    }
                }
            }
        }

        private bool EvaluateException(QueryExpression query, T? record)
        {
            throw new NotImplementedException();
        }

        // Пытается использовать индекс для выполнения запроса
        private bool TryUseIndex(QueryExpression query, out IAsyncEnumerable<T>? values)
        {
            values = null;
            if(query is BinaryExpression binary)
            {
                if(_indexes.TryGetValue(binary.Field, out var index))
                {
                    // В зависимости от оператора используем индекс
                    switch (binary.Operator)
                    {
                        case "=":
                            values = ExecuteEqualityIndexQuery(index, binary.Value);
                            return true;
                        case ">":
                        case ">=":
                        case "<":
                        case "<=":
                            values = ExecuteRangeIndexQuery(index, binary.Operator, binary.Value);
                            return true;
                        case "BETWEEN":
                            if (binary is BetweenExpression between)
                            {
                                values = ExecuteBetweenIndexQuery(index, between.MinValue, between.MaxValue);
                                return true;
                            }
                            break;
                    }
                }
            }

            return false;
        }

        // Выполняет запрос равенства с использованием индекса
        private async IAsyncEnumerable<T> ExecuteEqualityIndexQuery(IIndex index, object value)
        {
            var method = 
                GetType().GetMethod(nameof(ExecuteEqualityIndexQueryGeneric), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)
                ?.MakeGenericMethod(index.KeyType, index.ValueType, typeof(T));

            if (method != null)
            {
                var task = (IAsyncEnumerable<T>?)method.
                    Invoke(null, new object[] { index, value, _record });

                if (task != null)
                {
                    await foreach (var item in task)
                    {
                        yield return item;
                    }
                }
            }
        }

        // Generic версия запроса равенства
        private static async IAsyncEnumerable<TResult> ExecuteEqualityIndexQueryGeneric<TKey, TValue, TResult>(
                    IIndex<TKey, TValue> index, TKey key, IRecordStorage<TResult> storage)
                    where TKey : IComparable<TKey>
        {
            await foreach (var value in index.FindAsync(key))
            {
                // По идее нужно преобразовать TValue в RecordId и загрузить запись
                // Пока что оставим заглушку
                yield return default!;
            }
        }

       
        /// Выполняет запрос диапазона с использованием индекса
        private async IAsyncEnumerable<T> ExecuteRangeIndexQuery(IIndex index, string @operator, object value)
        {
            // Аналогично ExecuteEqualityIndexQuery, но для диапазонов
            yield break; // Заглушка
        }


        /// Выполняет запрос BETWEEN с использованием индекса
        private async IAsyncEnumerable<T> ExecuteBetweenIndexQuery(IIndex index, object minValue, object maxValue)
        {
            // Аналогично ExecuteEqualityIndexQuery, но для BETWEEN
            yield break; // Заглушка
        }


        /// Вычисляет выражение для записи
        private static bool EvaluateExpression(QueryExpression expression, T record)
        {
            // Компиляция выражений в runtime для производительности
            // Временная заглушка
            return true;
        }
    }


    /// Базовый класс для выражений запроса
    public abstract class QueryExpression
    {
        public abstract override string ToString();
    }


    /// Бинарное выражение (поле оператор значение)
    public class BinaryExpression : QueryExpression
    {
        public string Field { get; }
        public string Operator { get; }
        public object Value { get; }

        public BinaryExpression(string field, string @operator, object value)
        {
            Field = field;
            Operator = @operator;
            Value = value;
        }

        public override string ToString() => $"{Field} {Operator} {Value}";
    }


    /// Выражение BETWEEN
    public class BetweenExpression : QueryExpression
    {
        public string Field { get; }
        public object MinValue { get; }
        public object MaxValue { get; }

        public BetweenExpression(string field, object minValue, object maxValue)
        {
            Field = field;
            MinValue = minValue;
            MaxValue = maxValue;
        }

        public override string ToString() => $"{Field} BETWEEN {MinValue} AND {MaxValue}";
    }


}

