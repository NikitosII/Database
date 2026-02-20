using Database.Core.Interfaces;
using Database.Core.Models;

namespace Database.Indexing
{
    /// <summary>
    /// B-tree implementation for indexing data.
    /// Minimum degree t: each node holds between t-1 and 2t-1 keys.
    /// </summary>
    public class BTree<TKey, TValue> : IIndex<TKey, TValue> where TKey : IComparable<TKey>
    {
        private readonly ITreeNode<TKey, TValue> _nodeManager;
        private readonly int _degree;
        private readonly object _lock = new();

        public string FieldName { get; }
        public Type KeyType => typeof(TKey);
        public Type ValueType => typeof(TValue);

        public BTree(string fieldName, ITreeNode<TKey, TValue> nodeManager, int degree = 3)
        {
            FieldName = fieldName ?? throw new ArgumentNullException(nameof(fieldName));
            _nodeManager = nodeManager ?? throw new ArgumentNullException(nameof(nodeManager));
            _degree = degree >= 2 ? degree : throw new ArgumentException("Degree must be at least 2", nameof(degree));
        }

        // ----- Insert -----

        public void Insert(TKey key, TValue value)
        {
            lock (_lock)
            {
                var root = _nodeManager.GetRootNode();

                if (root.KeyCount == 2 * _degree - 1)
                {
                    // Root is full — split it
                    var newRoot = _nodeManager.CreateNewNode(isLeaf: false);
                    _nodeManager.MakeRoot(newRoot);
                    newRoot.Children.Add(root.Id);
                    SplitChild(newRoot, 0, root);
                    InsertNonFull(newRoot, key, value);
                }
                else
                {
                    InsertNonFull(root, key, value);
                }
            }
        }

        private void InsertNonFull(TreeNode<TKey, TValue> node, TKey key, TValue value)
        {
            var i = node.KeyCount - 1;

            if (node.IsLeaf)
            {
                while (i >= 0 && key.CompareTo(node.Keys[i]) < 0)
                    i--;

                node.Keys.Insert(i + 1, key);
                node.Values.Insert(i + 1, value);
                _nodeManager.SaveChanges(node);
            }
            else
            {
                while (i >= 0 && key.CompareTo(node.Keys[i]) < 0)
                    i--;
                i++;

                var child = _nodeManager.GetNode(node.Children[i]);

                if (child.KeyCount == 2 * _degree - 1)
                {
                    SplitChild(node, i, child);
                    if (key.CompareTo(node.Keys[i]) > 0)
                    {
                        i++;
                        child = _nodeManager.GetNode(node.Children[i]);
                    }
                }

                InsertNonFull(child, key, value);
            }
        }

        /// <summary>
        /// Splits a full child node. The median key is promoted to the parent.
        /// FIX: saves median key/value before modifying the child lists.
        /// </summary>
        private void SplitChild(TreeNode<TKey, TValue> parent, int index, TreeNode<TKey, TValue> child)
        {
            var newNode = _nodeManager.CreateNewNode(child.IsLeaf);

            // Save the median key/value BEFORE we mutate any list
            var medianKey = child.Keys[_degree - 1];
            var medianValue = child.Values[_degree - 1];

            // Copy the right half (keys after median) into newNode
            for (int j = 0; j < _degree - 1; j++)
            {
                newNode.Keys.Add(child.Keys[_degree + j]);
                newNode.Values.Add(child.Values[_degree + j]);
            }

            // If not a leaf, copy the right half of children
            if (!child.IsLeaf)
            {
                for (int j = 0; j < _degree; j++)
                    newNode.Children.Add(child.Children[_degree + j]);
            }

            // Trim child: keep only the first _degree-1 keys (everything before the median)
            // Remove from index (_degree-1) to the end: that's median + right half = _degree items
            child.Keys.RemoveRange(_degree - 1, _degree);
            child.Values.RemoveRange(_degree - 1, _degree);
            if (!child.IsLeaf)
                child.Children.RemoveRange(_degree, _degree);

            // Promote median into parent
            parent.Keys.Insert(index, medianKey);
            parent.Values.Insert(index, medianValue);
            parent.Children.Insert(index + 1, newNode.Id);

            _nodeManager.SaveChanges(parent);
            _nodeManager.SaveChanges(child);
            _nodeManager.SaveChanges(newNode);
        }

        // ----- Delete -----

        public bool Delete(TKey key, TValue value)
        {
            lock (_lock)
            {
                var root = _nodeManager.GetRootNode();
                var result = DeleteFromNode(root, key, value);

                // If root became empty and is not a leaf, promote its only child
                if (root.KeyCount == 0 && !root.IsLeaf)
                {
                    var newRoot = _nodeManager.GetNode(root.Children[0]);
                    _nodeManager.MakeRoot(newRoot);
                    _nodeManager.DeleteNode(root);
                }

                return result;
            }
        }

        private bool DeleteFromNode(TreeNode<TKey, TValue> node, TKey key, TValue value)
        {
            var idx = node.Keys.FindIndex(k => k.CompareTo(key) == 0);

            if (idx >= 0)
            {
                if (node.IsLeaf)
                {
                    node.Keys.RemoveAt(idx);
                    node.Values.RemoveAt(idx);
                    _nodeManager.SaveChanges(node);
                    return true;
                }
                return DeleteFromInternalNode(node, idx, key, value);
            }

            // Key not in this node — descend into the appropriate child
            if (node.IsLeaf)
                return false;

            var childIndex = FindChildIndex(node, key);
            var child = _nodeManager.GetNode(node.Children[childIndex]);

            if (child.KeyCount == _degree - 1)
            {
                var oldChildCount = node.Children.Count;
                FillChild(node, childIndex);
                // If a merge happened (child count decreased) and we merged with
                // the left sibling, the merged node is at childIndex - 1
                if (node.Children.Count < oldChildCount && childIndex > 0)
                    childIndex--;
                child = _nodeManager.GetNode(node.Children[childIndex]);
            }

            return DeleteFromNode(child, key, value);
        }

        private bool DeleteFromInternalNode(TreeNode<TKey, TValue> node, int keyIndex, TKey key, TValue value)
        {
            var leftChild = _nodeManager.GetNode(node.Children[keyIndex]);
            var rightChild = _nodeManager.GetNode(node.Children[keyIndex + 1]);

            if (leftChild.KeyCount >= _degree)
            {
                var pred = GetPredecessor(leftChild);
                node.Keys[keyIndex] = pred.Key;
                node.Values[keyIndex] = pred.Value;
                _nodeManager.SaveChanges(node);
                return DeleteFromNode(leftChild, pred.Key, pred.Value);
            }

            if (rightChild.KeyCount >= _degree)
            {
                var succ = GetSuccessor(rightChild);
                node.Keys[keyIndex] = succ.Key;
                node.Values[keyIndex] = succ.Value;
                _nodeManager.SaveChanges(node);
                return DeleteFromNode(rightChild, succ.Key, succ.Value);
            }

            // Merge left and right children
            MergeChildren(node, keyIndex);
            return DeleteFromNode(leftChild, key, value);
        }

        private int FindChildIndex(TreeNode<TKey, TValue> node, TKey key)
        {
            int idx = 0;
            while (idx < node.KeyCount && key.CompareTo(node.Keys[idx]) > 0)
                idx++;
            return idx;
        }

        private void FillChild(TreeNode<TKey, TValue> parent, int childIndex)
        {
            if (childIndex > 0)
            {
                var leftSibling = _nodeManager.GetNode(parent.Children[childIndex - 1]);
                if (leftSibling.KeyCount >= _degree)
                {
                    BorrowFromLeft(parent, childIndex, leftSibling);
                    return;
                }
            }

            if (childIndex < parent.Children.Count - 1)
            {
                var rightSibling = _nodeManager.GetNode(parent.Children[childIndex + 1]);
                if (rightSibling.KeyCount >= _degree)
                {
                    BorrowFromRight(parent, childIndex, rightSibling);
                    return;
                }
            }

            // Neither sibling can lend a key — merge
            if (childIndex > 0)
                MergeChildren(parent, childIndex - 1);
            else
                MergeChildren(parent, childIndex);
        }

        private void BorrowFromLeft(TreeNode<TKey, TValue> parent, int childIndex,
            TreeNode<TKey, TValue> leftSibling)
        {
            var child = _nodeManager.GetNode(parent.Children[childIndex]);

            child.Keys.Insert(0, parent.Keys[childIndex - 1]);
            child.Values.Insert(0, parent.Values[childIndex - 1]);

            parent.Keys[childIndex - 1] = leftSibling.Keys[^1];
            parent.Values[childIndex - 1] = leftSibling.Values[^1];

            if (!child.IsLeaf)
            {
                child.Children.Insert(0, leftSibling.Children[^1]);
                leftSibling.Children.RemoveAt(leftSibling.Children.Count - 1);
            }

            leftSibling.Keys.RemoveAt(leftSibling.Keys.Count - 1);
            leftSibling.Values.RemoveAt(leftSibling.Values.Count - 1);

            _nodeManager.SaveChanges(parent);
            _nodeManager.SaveChanges(leftSibling);
            _nodeManager.SaveChanges(child);
        }

        private void BorrowFromRight(TreeNode<TKey, TValue> parent, int childIndex,
            TreeNode<TKey, TValue> rightSibling)
        {
            var child = _nodeManager.GetNode(parent.Children[childIndex]);

            child.Keys.Add(parent.Keys[childIndex]);
            child.Values.Add(parent.Values[childIndex]);

            parent.Keys[childIndex] = rightSibling.Keys[0];
            parent.Values[childIndex] = rightSibling.Values[0];

            if (!child.IsLeaf)
            {
                child.Children.Add(rightSibling.Children[0]);
                rightSibling.Children.RemoveAt(0);
            }

            rightSibling.Keys.RemoveAt(0);
            rightSibling.Values.RemoveAt(0);

            _nodeManager.SaveChanges(parent);
            _nodeManager.SaveChanges(rightSibling);
            _nodeManager.SaveChanges(child);
        }

        private void MergeChildren(TreeNode<TKey, TValue> parent, int leftChildIndex)
        {
            var leftChild = _nodeManager.GetNode(parent.Children[leftChildIndex]);
            var rightChild = _nodeManager.GetNode(parent.Children[leftChildIndex + 1]);

            leftChild.Keys.Add(parent.Keys[leftChildIndex]);
            leftChild.Values.Add(parent.Values[leftChildIndex]);

            leftChild.Keys.AddRange(rightChild.Keys);
            leftChild.Values.AddRange(rightChild.Values);

            if (!leftChild.IsLeaf)
                leftChild.Children.AddRange(rightChild.Children);

            parent.Keys.RemoveAt(leftChildIndex);
            parent.Values.RemoveAt(leftChildIndex);
            parent.Children.RemoveAt(leftChildIndex + 1);

            _nodeManager.DeleteNode(rightChild);
            _nodeManager.SaveChanges(parent);
            _nodeManager.SaveChanges(leftChild);
        }

        private (TKey Key, TValue Value) GetPredecessor(TreeNode<TKey, TValue> node)
        {
            while (!node.IsLeaf)
                node = _nodeManager.GetNode(node.Children[^1]);
            return (node.Keys[^1], node.Values[^1]);
        }

        private (TKey Key, TValue Value) GetSuccessor(TreeNode<TKey, TValue> node)
        {
            while (!node.IsLeaf)
                node = _nodeManager.GetNode(node.Children[0]);
            return (node.Keys[0], node.Values[0]);
        }

        // ----- Find -----

        public IEnumerable<TValue> Find(TKey key)
        {
            var root = _nodeManager.GetRootNode();
            return FindInNode(root, key);
        }

        private IEnumerable<TValue> FindInNode(TreeNode<TKey, TValue> node, TKey key)
        {
            int i = 0;
            while (i < node.KeyCount && key.CompareTo(node.Keys[i]) > 0)
                i++;

            // Search the left child subtree at position i
            if (!node.IsLeaf)
            {
                foreach (var v in FindInNode(_nodeManager.GetNode(node.Children[i]), key))
                    yield return v;
            }

            // Yield all consecutive matching keys, searching subtrees in between
            while (i < node.KeyCount && key.CompareTo(node.Keys[i]) == 0)
            {
                yield return node.Values[i];
                i++;

                if (!node.IsLeaf)
                {
                    foreach (var v in FindInNode(_nodeManager.GetNode(node.Children[i]), key))
                        yield return v;
                }
            }
        }

        // ----- Range Find -----

        public IEnumerable<TValue> FindRange(TKey min, TKey max,
            bool inclusiveMin = true, bool inclusiveMax = true)
        {
            var root = _nodeManager.GetRootNode();
            return FindRangeInNode(root, min, max, inclusiveMin, inclusiveMax);
        }

        private IEnumerable<TValue> FindRangeInNode(TreeNode<TKey, TValue> node,
            TKey min, TKey max, bool inclusiveMin, bool inclusiveMax)
        {
            int i = 0;
            while (i < node.KeyCount &&
                   (inclusiveMin ? min.CompareTo(node.Keys[i]) > 0 : min.CompareTo(node.Keys[i]) >= 0))
                i++;

            while (i < node.KeyCount &&
                   (inclusiveMax ? max.CompareTo(node.Keys[i]) >= 0 : max.CompareTo(node.Keys[i]) > 0))
            {
                if (!node.IsLeaf)
                {
                    foreach (var v in FindRangeInNode(_nodeManager.GetNode(node.Children[i]),
                        min, max, inclusiveMin, inclusiveMax))
                        yield return v;
                }

                yield return node.Values[i];
                i++;
            }

            if (!node.IsLeaf && i < node.Children.Count)
            {
                foreach (var v in FindRangeInNode(_nodeManager.GetNode(node.Children[i]),
                    min, max, inclusiveMin, inclusiveMax))
                    yield return v;
            }
        }

        // ----- Min / Max -----

        public TKey GetMinKey()
        {
            var node = _nodeManager.GetRootNode();
            while (!node.IsLeaf)
                node = _nodeManager.GetNode(node.Children[0]);
            return node.Keys[0];
        }

        public TKey GetMaxKey()
        {
            var node = _nodeManager.GetRootNode();
            while (!node.IsLeaf)
                node = _nodeManager.GetNode(node.Children[^1]);
            return node.Keys[^1];
        }
    }
}
