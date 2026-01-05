namespace Database.Core.Models
{
    /// Узел B-дерева
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

