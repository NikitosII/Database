using System.Text.Json;

namespace Database.Serialization
{
    /// <summary>
    /// Generic JSON-based serializer. Kept for potential use with
    /// arbitrary record types beyond CowModel.
    /// </summary>
    public class JsonRecord<T>
    {
        private readonly JsonSerializerOptions _options;

        public JsonRecord(JsonSerializerOptions? options = null)
        {
            _options = options ?? new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                WriteIndented = false
            };
        }

        public byte[] Serialize(T record)
        {
            return JsonSerializer.SerializeToUtf8Bytes(record, _options);
        }

        public T? Deserialize(byte[] data)
        {
            return JsonSerializer.Deserialize<T>(data, _options);
        }
    }
}
