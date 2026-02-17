using Database.Core.Interfaces;

namespace Database.Storage
{
    /// <summary>
    /// Synchronous block-level file storage.
    /// Reads/writes fixed-size blocks to a data file.
    /// </summary>
    public class BlockStorage : IBlockStorage
    {
        private readonly FileStream _fileStream;
        private readonly int _blockSize;
        private bool _disposed;

        public int BlockSize => _blockSize;
        public int BlockCount => (int)(_fileStream.Length / _blockSize);

        public BlockStorage(string path, int blockSize = 4096)
        {
            _blockSize = blockSize;
            _fileStream = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(BlockStorage));
        }

        public byte[] ReadBlock(int blockId)
        {
            ThrowIfDisposed();
            var buffer = new byte[_blockSize];
            _fileStream.Seek((long)blockId * _blockSize, SeekOrigin.Begin);
            int bytesRead = _fileStream.Read(buffer, 0, _blockSize);
            if (bytesRead != _blockSize)
                throw new InvalidOperationException($"Incomplete block read: {bytesRead} bytes");
            return buffer;
        }

        public void WriteBlock(int blockId, byte[] data)
        {
            ThrowIfDisposed();
            if (data.Length != _blockSize)
                throw new ArgumentException($"Data must be exactly {_blockSize} bytes, got {data.Length}");

            _fileStream.Seek((long)blockId * _blockSize, SeekOrigin.Begin);
            _fileStream.Write(data, 0, _blockSize);
            _fileStream.Flush();
        }

        public int AllocateBlock()
        {
            ThrowIfDisposed();
            int newBlockId = BlockCount;
            _fileStream.SetLength(_fileStream.Length + _blockSize);
            return newBlockId;
        }

        public void FreeBlock(int blockId)
        {
            ThrowIfDisposed();
            // Tombstone: zero out the block
            var empty = new byte[_blockSize];
            WriteBlock(blockId, empty);
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _disposed = true;
                _fileStream.Dispose();
            }
        }
    }
}
