namespace Database.Core.Interfaces
{
    public interface IBlockStorage : IDisposable
    {
        byte[] ReadBlock(int blockId);
        void WriteBlock(int blockId, byte[] data);
        void FreeBlock(int blockId);
        int AllocateBlock();
        int BlockSize { get; }
        int BlockCount { get; }
    }
}
