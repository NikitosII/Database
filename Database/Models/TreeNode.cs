namespace Database.Core.Models
{
    /// <summary>
    /// A B-tree node holding keys, values, and child pointers.
    /// </summary>
    public class TreeNode<TKey, TValue>
    {
        public int Id { get; set; }
        public bool IsLeaf { get; set; }
        public List<TKey> Keys { get; set; } = new();
        public List<TValue> Values { get; set; } = new();
        public List<int> Children { get; set; } = new();
        public int KeyCount => Keys.Count;
    }
}
