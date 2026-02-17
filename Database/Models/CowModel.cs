namespace Database.Core.Models
{
    public class CowModel
    {
        public Guid Id { get; set; }
        public string Breed { get; set; } = string.Empty;
        public int Age { get; set; }
        public string Name { get; set; } = string.Empty;
        public byte[] DnaData { get; set; } = Array.Empty<byte>();
    }
}
