using System.Text;
using Database.Core.Models;

namespace Database.Serialization
{
    /// <summary>
    /// Binary serializer for CowModel.
    /// Handles variable-length fields (Breed, Name, DnaData) using length-prefixed encoding.
    ///
    /// Binary format:
    /// 
    /// │ Id        : 16 bytes  (Guid)         │
    /// │ Age       :  4 bytes  (int32, LE)    │
    /// │ BreedLen  :  4 bytes  (int32, LE)    │
    /// │ Breed     :  N bytes  (UTF-8)        │
    /// │ NameLen   :  4 bytes  (int32, LE)    │
    /// │ Name      :  N bytes  (UTF-8)        │
    /// │ DnaLen    :  4 bytes  (int32, LE)    │
    /// │ DnaData   :  N bytes  (raw)          │
    /// 
    /// </summary>
    public static class CowSerializer
    {
        public static byte[] Serialize(CowModel cow)
        {
            var breedBytes = Encoding.UTF8.GetBytes(cow.Breed ?? string.Empty);
            var nameBytes = Encoding.UTF8.GetBytes(cow.Name ?? string.Empty);
            var dnaBytes = cow.DnaData ?? Array.Empty<byte>();

            // Total: 16 (Guid) + 4 (Age) + 4+N (Breed) + 4+N (Name) + 4+N (DnaData)
            int totalSize = 16 + 4 + (4 + breedBytes.Length) + (4 + nameBytes.Length) + (4 + dnaBytes.Length);
            var buffer = new byte[totalSize];
            int offset = 0;

            // Id (16 bytes)
            cow.Id.ToByteArray().CopyTo(buffer, offset);
            offset += 16;

            // Age (4 bytes)
            BitConverter.GetBytes(cow.Age).CopyTo(buffer, offset);
            offset += 4;

            // Breed (length-prefixed)
            BitConverter.GetBytes(breedBytes.Length).CopyTo(buffer, offset);
            offset += 4;
            breedBytes.CopyTo(buffer, offset);
            offset += breedBytes.Length;

            // Name (length-prefixed)
            BitConverter.GetBytes(nameBytes.Length).CopyTo(buffer, offset);
            offset += 4;
            nameBytes.CopyTo(buffer, offset);
            offset += nameBytes.Length;

            // DnaData (length-prefixed)
            BitConverter.GetBytes(dnaBytes.Length).CopyTo(buffer, offset);
            offset += 4;
            dnaBytes.CopyTo(buffer, offset);

            return buffer;
        }

        public static CowModel Deserialize(byte[] data)
        {
            int offset = 0;
            var cow = new CowModel();

            // Id
            cow.Id = new Guid(data.AsSpan(offset, 16));
            offset += 16;

            // Age
            cow.Age = BitConverter.ToInt32(data, offset);
            offset += 4;

            // Breed
            int breedLen = BitConverter.ToInt32(data, offset);
            offset += 4;
            cow.Breed = Encoding.UTF8.GetString(data, offset, breedLen);
            offset += breedLen;

            // Name
            int nameLen = BitConverter.ToInt32(data, offset);
            offset += 4;
            cow.Name = Encoding.UTF8.GetString(data, offset, nameLen);
            offset += nameLen;

            // DnaData
            int dnaLen = BitConverter.ToInt32(data, offset);
            offset += 4;
            cow.DnaData = new byte[dnaLen];
            Array.Copy(data, offset, cow.DnaData, 0, dnaLen);

            return cow;
        }
    }
}
