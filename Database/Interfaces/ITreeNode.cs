using Database.Core.Models;

namespace Database.Core.Interfaces
{
    public interface ITreeNode<TKey, TValue>
    {
        /// Получает корневой узел
        Task<TreeNode<TKey, TValue>> GetRootNodeAsync();

        /// Устанавливает новый корневой узел
        Task MakeRootAsync(TreeNode<TKey, TValue> node);

        /// Создает новый узел
        Task<TreeNode<TKey, TValue>> CreateNewNodeAsync(bool isLeaf);

        /// Получает узел по идентификатору
        Task<TreeNode<TKey, TValue>> GetNodeAsync(int nodeId);

        /// Сохраняет изменения в узле
        Task SaveChangesAsync(TreeNode<TKey, TValue> node);

        /// Удаляет узел
        Task DeleteNodeAsync(TreeNode<TKey, TValue> node);
    }
}
