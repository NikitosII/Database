using System.Buffers;
using System.IO.Pipelines;
using System.Threading.Channels;
using Database.Core.Interfaces;
using Microsoft.Extensions.Logging;


namespace Database.Storage
{
    public class BlockStorage : IBlockStorage 
    {
        private readonly FileStream _fileStream;
        private readonly int _blockSize;
        private readonly Pipe _pipe;
        private readonly CancellationTokenSource _tokenSourse = new();
        private bool _disposed = false;
        private readonly ILogger<BlockStorage> _logger;
        private readonly record struct WriteOperation(int BlockId, ReadOnlyMemory<byte> Data);
        private readonly Channel<WriteOperation> _writeChannel;

        public int BlockSize => _blockSize;


        public BlockStorage(string path, int blockSize = 8192, ILogger<BlockStorage>? logger = null)
        {
            _blockSize = blockSize;
            _logger = logger;
            _fileStream = File.Open(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
            _writeChannel = Channel.CreateBounded<WriteOperation>(1000);
            
        }

        private void IfDispose()
        {
            if(_disposed)
                throw new ObjectDisposedException(nameof(BlockStorage));
        }

        public async ValueTask<Memory<byte>> ReadeBlockAsync(int blockId)
        {
            IfDispose();

            var position = (long)blockId * _blockSize;
            _fileStream.Seek(position, SeekOrigin.Begin);
            var buffer = ArrayPool<byte>.Shared.Rent(_blockSize);
            var bytes = await _fileStream.ReadAsync(buffer.AsMemory(0, _blockSize));

            if (bytes != _blockSize)
                throw new InvalidOperationException($"Incomplete block : {bytes} bytes");
            return new Memory<byte>(buffer, 0, _blockSize);

        }

        public ValueTask WriteeBlockAsync(int blockId, ReadOnlyMemory<byte> data)
        {
            IfDispose() ;

            if (data.Length != _blockSize)
                throw new ArgumentException($"DataSize must be {_blockSize} bytes");

            var opration = new WriteOperation(blockId, data);
            return _writeChannel.Writer.WriteAsync(opration);
        }

        public async Task ProcessAsync(CancellationToken token)
        {
            await foreach (var operation in _writeChannel.Reader.ReadAllAsync(token))
            {
                try
                {
                    var position = (long)operation.BlockId * _blockSize;
                    _fileStream.Seek(position, SeekOrigin.Begin);
                    await _fileStream.WriteAsync(operation.Data, token);
                    await _fileStream.FlushAsync(token);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to write block", operation.BlockId);
                }
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (!_disposed)
            {
                _disposed = true;
                _writeChannel.Writer.Complete();
                _tokenSourse.Cancel();
                await _fileStream.DisposeAsync();
                _tokenSourse.Dispose();
            }
        }

        public ValueTask FreeBlockAsync(int blockId)
        {
            // помечаем блок как свободный
            IfDispose();
            return ValueTask.CompletedTask;
        }

        public async ValueTask<int> AllocateBlockAsync()
        {
            IfDispose();

            // Увеличиваем файл на один блок и возвращаем ID нового блока
            var fileLength = _fileStream.Length;
            var newBlockId = (int)(fileLength / _blockSize);

            _fileStream.SetLength(fileLength + _blockSize);
            return newBlockId;
        }

    }
}
