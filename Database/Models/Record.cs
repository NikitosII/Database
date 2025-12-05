
namespace Database.Core.Models
{
    public record struct Record(int val)
    {
        public static readonly Record Empty = new(-1);
        public bool isEmpty () { return (val < 0); }
    }
}
