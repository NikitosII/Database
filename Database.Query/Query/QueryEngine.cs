using Database.Core.Interfaces;
using Database.Core.Models;
using System.Collections.Frozen;
using System.Reflection;

namespace Database.Query
{
    /// <summary>
    /// Executes queries using indexes when possible, falling back to full table scan.
    /// Uses dynamic typing and reflection to bridge the non-generic IIndex with
    /// the generic IIndex&lt;TKey,TValue&gt;.
    /// </summary>
    public class QueryEngine
    {
        private readonly IRecordStorage _storage;
        private readonly FrozenDictionary<string, IIndex> _indexes;

        public QueryEngine(IRecordStorage storage, IEnumerable<IIndex> indexes)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _indexes = indexes?.ToFrozenDictionary(x => x.FieldName)
                ?? throw new ArgumentNullException(nameof(indexes));
        }

        public IEnumerable<byte[]> Execute(QueryExpression query)
        {
            if (TryUseIndex(query, out var results))
                return results!;

            // Full table scan with filter
            return _storage.ScanAll()
                .Where(r => EvaluateExpression(query, r.Data))
                .Select(r => r.Data);
        }

        private bool TryUseIndex(QueryExpression query, out IEnumerable<byte[]>? values)
        {
            values = null;

            if (query is BinaryExpression binary &&
                _indexes.TryGetValue(binary.Field, out var index))
            {
                switch (binary.Operator)
                {
                    case "=":
                        values = ExecuteEqualityQuery(index, binary.Value);
                        return true;
                    case "BETWEEN" when binary is BetweenExpression between:
                        values = ExecuteRangeQuery(index, between.MinValue, between.MaxValue);
                        return true;
                }
            }

            return false;
        }

        private IEnumerable<byte[]> ExecuteEqualityQuery(IIndex index, object key)
        {
            // Use reflection to call Find on the generic index
            var findMethod = index.GetType().GetMethod("Find");
            if (findMethod == null) yield break;

            var result = findMethod.Invoke(index, new[] { key }) as System.Collections.IEnumerable;
            if (result == null) yield break;

            foreach (var location in result)
            {
                if (location is RecordLocation loc)
                    yield return _storage.Read(loc);
            }
        }

        private IEnumerable<byte[]> ExecuteRangeQuery(IIndex index, object min, object max)
        {
            var rangeMethod = index.GetType().GetMethod("FindRange");
            if (rangeMethod == null) yield break;

            var result = rangeMethod.Invoke(index, new[] { min, max, (object)true, (object)true })
                as System.Collections.IEnumerable;
            if (result == null) yield break;

            foreach (var location in result)
            {
                if (location is RecordLocation loc)
                    yield return _storage.Read(loc);
            }
        }

        private static bool EvaluateExpression(QueryExpression expression, byte[] data)
        {
            // For a full table scan, we would need to deserialize and check fields.
            // This is a placeholder — the CowDatabase uses indexes directly.
            return true;
        }
    }

    // ----- Query expression types -----

    public abstract class QueryExpression
    {
        public abstract override string ToString();
    }

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

    public class BetweenExpression : BinaryExpression
    {
        public object MinValue { get; }
        public object MaxValue { get; }

        public BetweenExpression(string field, object minValue, object maxValue)
            : base(field, "BETWEEN", minValue)
        {
            MinValue = minValue;
            MaxValue = maxValue;
        }

        public override string ToString() => $"{Field} BETWEEN {MinValue} AND {MaxValue}";
    }
}
