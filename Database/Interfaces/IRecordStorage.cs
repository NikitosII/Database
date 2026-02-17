using Database.Core.Models;

namespace Database.Core.Interfaces
{
    public interface IRecordStorage : IDisposable
    {
        RecordLocation Insert(byte[] data);
        byte[] Read(RecordLocation location);
        void Update(RecordLocation oldLocation, byte[] newData, out RecordLocation newLocation);
        void Delete(RecordLocation location);
        IEnumerable<(RecordLocation Location, byte[] Data)> ScanAll();
    }
}
