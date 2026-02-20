namespace Database.Core.Models
{
    /// <summary>
    /// Composite key for the secondary index on (Breed, Age).
    /// Compared lexicographically: first by Breed, then by Age.
    /// </summary>
    public readonly struct BreedAgeKey : IComparable<BreedAgeKey>, IEquatable<BreedAgeKey>
    {
        public string Breed { get; }
        public int Age { get; }

        public BreedAgeKey(string breed, int age)
        {
            Breed = breed;
            Age = age;
        }

        public int CompareTo(BreedAgeKey other)
        {
            int cmp = string.Compare(Breed, other.Breed, StringComparison.Ordinal);
            if (cmp != 0) return cmp;
            return Age.CompareTo(other.Age);
        }

        public bool Equals(BreedAgeKey other) =>
            Breed == other.Breed && Age == other.Age;

        public override bool Equals(object? obj) =>
            obj is BreedAgeKey other && Equals(other);

        public override int GetHashCode() =>
            HashCode.Combine(Breed, Age);

        public override string ToString() => $"({Breed}, {Age})";
    }
}
