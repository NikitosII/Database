
namespace Database.Core.Interfaces
{
    public interface IBlockStorage : IAsyncDisposable
    {
        ValueTask<Memory<byte>> ReadeBlockAsync(int blockId);
        ValueTask WriteeBlockAsync(int blockId, ReadOnlyMemory<byte> data);
        ValueTask FreeBlockAsync(int blockId);
        ValueTask<int> AllocateBlockAsync();
        int BlockSize { get; }
    }
}
