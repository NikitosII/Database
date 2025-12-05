using Database.Core.Models;

namespace Database.Core.Interfaces
{
    public interface IRecordStorage<T> : IAsyncDisposable
    {
        ValueTask<Record> InsertAsync(T record);
        ValueTask<T?> GetAsync(Record id);
        ValueTask<bool> UpdateAsync(Record id, T record);
        ValueTask<bool> DeleteAsync();
        IAsyncEnumerable<T> ScanAsync();
    }
}
