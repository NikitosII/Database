
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Database.Serialization.Serialization
{
    public class JsonRecord<T> 
    {
        private readonly JsonSerializerOptions _options;
        private readonly JsonTypeInfo<T> _info;

        public JsonRecord(JsonSerializerOptions? options = null)
        {
            _options = options ?? new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                WriteIndented = false
            };

            _info = JsonSerializerOptions.Default.GetTypeInfo(typeof(T)) 
                as JsonTypeInfo<T> ?? throw new InvalidOperationException($"No type info for {typeof(T)}");
        }

        public int GetSize(T record)
        {
            return JsonSerializer.SerializeToUtf8Bytes(record, _info).Length;
        }

        public void Serialize(T record, Span<byte> buffer)
        {
            JsonSerializer.SerializeToUtf8Bytes(record, _info).CopyTo(buffer);
        }

        public T Deserialize(Span<byte> buffer)
        {
            return JsonSerializer.Deserialize<T>(buffer, _info);
        }
    }
}
