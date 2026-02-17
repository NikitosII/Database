using Database.Core.Models;

namespace Database.Core.Interfaces
{
    public interface ITreeNode<TKey, TValue>
    {
        
        TreeNode<TKey, TValue> GetRootNode();
        void MakeRoot(TreeNode<TKey, TValue> node);
        TreeNode<TKey, TValue> CreateNewNode(bool isLeaf);
        TreeNode<TKey, TValue> GetNode(int nodeId);
        void SaveChanges(TreeNode<TKey, TValue> node);
        void DeleteNode(TreeNode<TKey, TValue> node);
    }
}
