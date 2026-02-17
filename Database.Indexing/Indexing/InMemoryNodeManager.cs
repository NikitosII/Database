using Database.Core.Interfaces;
using Database.Core.Models;

namespace Database.Indexing
{
    /// <summary>
    /// In-memory implementation of ITreeNode.
    /// Stores all B-tree nodes in a dictionary. Indexes are rebuilt from
    /// the data file on startup, so persistence of tree nodes is not required.
    /// </summary>
    public class InMemoryNodeManager<TKey, TValue> : ITreeNode<TKey, TValue>
    {
        private readonly Dictionary<int, TreeNode<TKey, TValue>> _nodes = new();
        private int _rootId;
        private int _nextId;

        public InMemoryNodeManager()
        {
            // Create the initial empty root (a leaf)
            var root = new TreeNode<TKey, TValue>
            {
                Id = _nextId++,
                IsLeaf = true
            };
            _nodes[root.Id] = root;
            _rootId = root.Id;
        }

        public TreeNode<TKey, TValue> GetRootNode() => _nodes[_rootId];

        public void MakeRoot(TreeNode<TKey, TValue> node) => _rootId = node.Id;

        public TreeNode<TKey, TValue> CreateNewNode(bool isLeaf)
        {
            var node = new TreeNode<TKey, TValue>
            {
                Id = _nextId++,
                IsLeaf = isLeaf
            };
            _nodes[node.Id] = node;
            return node;
        }

        public TreeNode<TKey, TValue> GetNode(int nodeId)
        {
            if (!_nodes.TryGetValue(nodeId, out var node))
                throw new KeyNotFoundException($"Node {nodeId} not found");
            return node;
        }

        public void SaveChanges(TreeNode<TKey, TValue> node)
        {
            _nodes[node.Id] = node;
        }

        public void DeleteNode(TreeNode<TKey, TValue> node)
        {
            _nodes.Remove(node.Id);
        }
    }
}
