// Indexing/BTree.cs
using Database.Core.Interfaces;
using Database.Core.Models;
using Microsoft.Extensions.Logging;
using OwnDatabase.Indexing.Interfaces;

namespace OwnDatabase.Indexing
{
    /// Реализация B-дерева для индексации данных
    public class BTree<TKey, TValue> : IIndex<TKey, TValue> where TKey : IComparable<TKey>
    {
        private readonly ITreeNode<TKey, TValue> _nodeManager;
        private readonly ILogger<BTree<TKey, TValue>> _logger;
        private readonly int _degree;
        private readonly SemaphoreSlim _lock = new(1, 1);

        public string FieldName { get; }
        public Type KeyType => typeof(TKey);
        public Type ValueType => typeof(TValue);

        /// <summary>
        /// Создает новый экземпляр B-дерева
        /// </summary>
        /// <param name="fieldName">Имя индексируемого поля</param>
        /// <param name="nodeManager">Менеджер узлов дерева</param>
        /// <param name="degree">Минимальная степень дерева (t). Узел содержит от t-1 до 2t-1 ключей</param>
        /// <param name="logger">Логгер</param>
        public BTree(string fieldName, ITreeNode<TKey, TValue> nodeManager, int degree = 3,
            ILogger<BTree<TKey, TValue>>? logger = null)
        {
            FieldName = fieldName ?? throw new ArgumentNullException(nameof(fieldName));
            _nodeManager = nodeManager ?? throw new ArgumentNullException(nameof(nodeManager));
            _degree = degree >= 2 ? degree : throw new ArgumentException("Degree must be at least 2", nameof(degree));
            _logger = logger;
        }

        /// Вставляет пару ключ-значение в дерево
        public async ValueTask InsertAsync(TKey key, TValue value)
        {
            await _lock.WaitAsync();
            try
            {
                var root = await _nodeManager.GetRootNodeAsync();

                // Если корневой узел полон, разделяем его
                if (root.KeyCount == 2 * _degree - 1)
                {
                    var newRoot = await _nodeManager.CreateNewNodeAsync(isLeaf: false);
                    await _nodeManager.MakeRootAsync(newRoot);
                    newRoot.Children.Add(root.Id);
                    await SplitChildAsync(newRoot, 0, root);
                    await InsertNonFullAsync(newRoot, key, value);
                }
                else
                {
                    await InsertNonFullAsync(root, key, value);
                }

                _logger?.LogDebug("Inserted key {Key} with value {Value} into BTree", key, value);
            }
            finally
            {
                _lock.Release();
            }
        }

        /// Вставляет ключ в неполный узел
        private async Task InsertNonFullAsync(TreeNode<TKey, TValue> node, TKey key, TValue value)
        {
            var i = node.KeyCount - 1;

            if (node.IsLeaf)
            {
                // Вставляем ключ в листовой узел
                while (i >= 0 && key.CompareTo(node.Keys[i]) < 0)
                {
                    i--;
                }
                node.Keys.Insert(i + 1, key);
                node.Values.Insert(i + 1, value);
                await _nodeManager.SaveChangesAsync(node);
            }
            else
            {
                // Находим дочерний узел для вставки
                while (i >= 0 && key.CompareTo(node.Keys[i]) < 0)
                {
                    i--;
                }
                i++;

                var child = await _nodeManager.GetNodeAsync(node.Children[i]);

                // Если дочерний узел полон, разделяем его
                if (child.KeyCount == 2 * _degree - 1)
                {
                    await SplitChildAsync(node, i, child);
                    if (key.CompareTo(node.Keys[i]) > 0)
                    {
                        i++;
                        child = await _nodeManager.GetNodeAsync(node.Children[i]);
                    }
                }

                await InsertNonFullAsync(child, key, value);
            }
        }

        /// Разделяет дочерний узел
        private async Task SplitChildAsync(TreeNode<TKey, TValue> parent, int index, TreeNode<TKey, TValue> child)
        {
            var newNode = await _nodeManager.CreateNewNodeAsync(child.IsLeaf);

            // Копируем вторую половину ключей и значений из child в newNode
            for (int j = 0; j < _degree - 1; j++)
            {
                newNode.Keys.Add(child.Keys[_degree + j]);
                newNode.Values.Add(child.Values[_degree + j]);
            }

            // Если child не лист, копируем детей
            if (!child.IsLeaf)
            {
                for (int j = 0; j < _degree; j++)
                {
                    newNode.Children.Add(child.Children[_degree + j]);
                }
            }

            // Удаляем скопированные ключи и детей из child
            child.Keys.RemoveRange(_degree - 1, _degree);
            child.Values.RemoveRange(_degree - 1, _degree);
            if (!child.IsLeaf)
            {
                child.Children.RemoveRange(_degree, _degree);
            }

            // Вставляем средний ключ из child в parent
            parent.Keys.Insert(index, child.Keys[_degree - 1]);
            parent.Values.Insert(index, child.Values[_degree - 1]);
            parent.Children.Insert(index + 1, newNode.Id);

            // Удаляем средний ключ из child
            child.Keys.RemoveAt(_degree - 1);
            child.Values.RemoveAt(_degree - 1);

            // Сохраняем изменения
            await _nodeManager.SaveChangesAsync(parent);
            await _nodeManager.SaveChangesAsync(child);
            await _nodeManager.SaveChangesAsync(newNode);

            _logger?.LogDebug("Split child node {ChildId} of parent {ParentId}", child.Id, parent.Id);
        }

        /// Удаляет значение из индекса по ключу
        public async ValueTask<bool> DeleteAsync(TKey key, TValue value)
        {
            await _lock.WaitAsync();
            try
            {
                var root = await _nodeManager.GetRootNodeAsync();
                var result = await DeleteFromNodeAsync(root, key, value);

                // Если корневой узел стал пустым и не является листом, удаляем его
                if (root.KeyCount == 0 && !root.IsLeaf)
                {
                    var newRoot = await _nodeManager.GetNodeAsync(root.Children[0]);
                    await _nodeManager.MakeRootAsync(newRoot);
                    await _nodeManager.DeleteNodeAsync(root);
                }

                _logger?.LogDebug("Deleted key {Key} with value {Value} from BTree: {Result}", key, value, result);
                return result;
            }
            finally
            {
                _lock.Release();
            }
        }

        /// Удаляет ключ из узла
        private async Task<bool> DeleteFromNodeAsync(TreeNode<TKey, TValue> node, TKey key, TValue value)
        {
            var idx = node.Keys.FindIndex(k => k.CompareTo(key) == 0);

            if (idx >= 0)
            {
                // Ключ найден в этом узле
                var valueIdx = node.Values.IndexOf(value);
                if (valueIdx < 0) return false;

                if (node.IsLeaf)
                {
                    // Удаляем из листового узла
                    node.Keys.RemoveAt(idx);
                    node.Values.RemoveAt(valueIdx);
                    await _nodeManager.SaveChangesAsync(node);
                    return true;
                }
                else
                {
                    // Узел не лист
                    return await DeleteFromInternalNodeAsync(node, idx, value);
                }
            }
            else
            {
                // Ключ не найден в этом узле
                if (node.IsLeaf)
                {
                    return false;
                }

                // Находим дочерний узел, который может содержать ключ
                var childIndex = FindChildIndex(node, key);
                var child = await _nodeManager.GetNodeAsync(node.Children[childIndex]);

                // Если у дочернего узла минимальное количество ключей, восполняем его
                if (child.KeyCount == _degree - 1)
                {
                    await FillChildAsync(node, childIndex);
                    child = await _nodeManager.GetNodeAsync(node.Children[childIndex]);
                }

                return await DeleteFromNodeAsync(child, key, value);
            }
        }

        /// Находит индекс дочернего узла для заданного ключа
        private int FindChildIndex(TreeNode<TKey, TValue> node, TKey key)
        {
            var idx = 0;
            while (idx < node.KeyCount && key.CompareTo(node.Keys[idx]) > 0)
            {
                idx++;
            }
            return idx;
        }

        /// Заполняет дочерний узел, если в нем слишком мало ключей
        private async Task FillChildAsync(TreeNode<TKey, TValue> parent, int childIndex)
        {
            var child = await _nodeManager.GetNodeAsync(parent.Children[childIndex]);

            if (childIndex > 0)
            {
                var leftSibling = await _nodeManager.GetNodeAsync(parent.Children[childIndex - 1]);
                if (leftSibling.KeyCount >= _degree)
                {
                    await BorrowFromLeftSiblingAsync(parent, childIndex, leftSibling, child);
                    return;
                }
            }

            if (childIndex < parent.Children.Count - 1)
            {
                var rightSibling = await _nodeManager.GetNodeAsync(parent.Children[childIndex + 1]);
                if (rightSibling.KeyCount >= _degree)
                {
                    await BorrowFromRightSiblingAsync(parent, childIndex, child, rightSibling);
                    return;
                }
            }

            // Если ни один сосед не может одолжить ключ, объединяем
            if (childIndex > 0)
            {
                await MergeChildrenAsync(parent, childIndex - 1);
            }
            else
            {
                await MergeChildrenAsync(parent, childIndex);
            }
        }

        /// Удаляет ключ из внутреннего узла
        private async Task<bool> DeleteFromInternalNodeAsync(TreeNode<TKey, TValue> node, int keyIndex, TValue value)
        {
            // Проверяем, есть ли значение в узле
            var valueIdx = node.Values.IndexOf(value);
            if (valueIdx < 0) return false;

            var leftChild = await _nodeManager.GetNodeAsync(node.Children[keyIndex]);
            var rightChild = await _nodeManager.GetNodeAsync(node.Children[keyIndex + 1]);

            if (leftChild.KeyCount >= _degree)
            {
                // Берем наибольший ключ из левого дочернего узла
                var predecessor = await GetPredecessorAsync(leftChild);
                node.Keys[keyIndex] = predecessor.Key;
                node.Values[valueIdx] = predecessor.Value;
                await _nodeManager.SaveChangesAsync(node);
                return await DeleteFromNodeAsync(leftChild, predecessor.Key, predecessor.Value);
            }
            else if (rightChild.KeyCount >= _degree)
            {
                // Берем наименьший ключ из правого дочернего узла
                var successor = await GetSuccessorAsync(rightChild);
                node.Keys[keyIndex] = successor.Key;
                node.Values[valueIdx] = successor.Value;
                await _nodeManager.SaveChangesAsync(node);
                return await DeleteFromNodeAsync(rightChild, successor.Key, successor.Value);
            }
            else
            {
                // Объединяем два дочерних узла
                await MergeChildrenAsync(node, keyIndex);
                return await DeleteFromNodeAsync(leftChild, node.Keys[keyIndex], value);
            }
        }

        /// Находит наибольшую пару ключ-значение в поддереве
        private async Task<(TKey Key, TValue Value)> GetPredecessorAsync(TreeNode<TKey, TValue> node)
        {
            while (!node.IsLeaf)
            {
                node = await _nodeManager.GetNodeAsync(node.Children[^1]);
            }
            return (node.Keys[^1], node.Values[^1]);
        }

        /// Находит наименьшую пару ключ-значение в поддереве
        private async Task<(TKey Key, TValue Value)> GetSuccessorAsync(TreeNode<TKey, TValue> node)
        {
            while (!node.IsLeaf)
            {
                node = await _nodeManager.GetNodeAsync(node.Children[0]);
            }
            return (node.Keys[0], node.Values[0]);
        }

        /// Заимствует ключ у левого соседа
        private async Task BorrowFromLeftSiblingAsync(TreeNode<TKey, TValue> parent, int childIndex,
            TreeNode<TKey, TValue> leftSibling, TreeNode<TKey, TValue> child)
        {
            // Перемещаем ключ из родителя в ребенка
            child.Keys.Insert(0, parent.Keys[childIndex - 1]);
            child.Values.Insert(0, parent.Values[childIndex - 1]);

            // Заменяем ключ в родителе на ключ из левого соседа
            parent.Keys[childIndex - 1] = leftSibling.Keys[^1];
            parent.Values[childIndex - 1] = leftSibling.Values[^1];

            // Если не листья, перемещаем последнего ребенка левого соседа
            if (!child.IsLeaf)
            {
                child.Children.Insert(0, leftSibling.Children[^1]);
                leftSibling.Children.RemoveAt(leftSibling.Children.Count - 1);
            }

            // Удаляем перемещенный ключ из левого соседа
            leftSibling.Keys.RemoveAt(leftSibling.Keys.Count - 1);
            leftSibling.Values.RemoveAt(leftSibling.Values.Count - 1);

            await _nodeManager.SaveChangesAsync(parent);
            await _nodeManager.SaveChangesAsync(leftSibling);
            await _nodeManager.SaveChangesAsync(child);
        }

        /// Заимствует ключ у правого соседа
        private async Task BorrowFromRightSiblingAsync(TreeNode<TKey, TValue> parent, int childIndex,
            TreeNode<TKey, TValue> child, TreeNode<TKey, TValue> rightSibling)
        {
            // Перемещаем ключ из родителя в ребенка
            child.Keys.Add(parent.Keys[childIndex]);
            child.Values.Add(parent.Values[childIndex]);

            // Заменяем ключ в родителе на ключ из правого соседа
            parent.Keys[childIndex] = rightSibling.Keys[0];
            parent.Values[childIndex] = rightSibling.Values[0];

            // Если не листья, перемещаем первого ребенка правого соседа
            if (!child.IsLeaf)
            {
                child.Children.Add(rightSibling.Children[0]);
                rightSibling.Children.RemoveAt(0);
            }

            // Удаляем перемещенный ключ из правого соседа
            rightSibling.Keys.RemoveAt(0);
            rightSibling.Values.RemoveAt(0);

            await _nodeManager.SaveChangesAsync(parent);
            await _nodeManager.SaveChangesAsync(rightSibling);
            await _nodeManager.SaveChangesAsync(child);
        }

        /// Объединяет двух детей родительского узла
        private async Task MergeChildrenAsync(TreeNode<TKey, TValue> parent, int leftChildIndex)
        {
            var leftChild = await _nodeManager.GetNodeAsync(parent.Children[leftChildIndex]);
            var rightChild = await _nodeManager.GetNodeAsync(parent.Children[leftChildIndex + 1]);

            // Добавляем ключ из родителя в левого ребенка
            leftChild.Keys.Add(parent.Keys[leftChildIndex]);
            leftChild.Values.Add(parent.Values[leftChildIndex]);

            // Копируем ключи и значения из правого ребенка
            leftChild.Keys.AddRange(rightChild.Keys);
            leftChild.Values.AddRange(rightChild.Values);

            // Если не листья, копируем детей
            if (!leftChild.IsLeaf)
            {
                leftChild.Children.AddRange(rightChild.Children);
            }

            // Удаляем ключ и правого ребенка из родителя
            parent.Keys.RemoveAt(leftChildIndex);
            parent.Values.RemoveAt(leftChildIndex);
            parent.Children.RemoveAt(leftChildIndex + 1);

            // Удаляем правый узел
            await _nodeManager.DeleteNodeAsync(rightChild);
            await _nodeManager.SaveChangesAsync(parent);
            await _nodeManager.SaveChangesAsync(leftChild);
        }

        /// Находит все значения по ключу
        public async IAsyncEnumerable<TValue> FindAsync(TKey key)
        {
            var root = await _nodeManager.GetRootNodeAsync();
            await foreach (var value in FindInNodeAsync(root, key))
            {
                yield return value;
            }
        }

        /// Находит значения в узле
        private async IAsyncEnumerable<TValue> FindInNodeAsync(TreeNode<TKey, TValue> node, TKey key)
        {
            var i = 0;
            while (i < node.KeyCount && key.CompareTo(node.Keys[i]) > 0)
            {
                i++;
            }

            if (i < node.KeyCount && key.CompareTo(node.Keys[i]) == 0)
            {
                yield return node.Values[i];
            }

            if (!node.IsLeaf)
            {
                var child = await _nodeManager.GetNodeAsync(node.Children[i]);
                await foreach (var value in FindInNodeAsync(child, key))
                {
                    yield return value;
                }
            }
        }

        /// Находит значения в диапазоне ключей
        public async IAsyncEnumerable<TValue> FindRangeAsync(TKey min, TKey max,
            bool inclusiveMin = true, bool inclusiveMax = true)
        {
            var root = await _nodeManager.GetRootNodeAsync();
            await foreach (var value in FindRangeInNodeAsync(root, min, max, inclusiveMin, inclusiveMax))
            {
                yield return value;
            }
        }

        /// Находит значения в диапазоне в узле
        private async IAsyncEnumerable<TValue> FindRangeInNodeAsync(TreeNode<TKey, TValue> node,
            TKey min, TKey max, bool inclusiveMin, bool inclusiveMax)
        {
            var i = 0;
            while (i < node.KeyCount &&
                   (inclusiveMin ? min.CompareTo(node.Keys[i]) > 0 : min.CompareTo(node.Keys[i]) >= 0))
            {
                i++;
            }

            while (i < node.KeyCount &&
                   (inclusiveMax ? max.CompareTo(node.Keys[i]) >= 0 : max.CompareTo(node.Keys[i]) > 0))
            {
                if (!node.IsLeaf)
                {
                    var child = await _nodeManager.GetNodeAsync(node.Children[i]);
                    await foreach (var value in FindRangeInNodeAsync(child, min, max, inclusiveMin, inclusiveMax))
                    {
                        yield return value;
                    }
                }

                yield return node.Values[i];
                i++;
            }

            if (!node.IsLeaf && i < node.Children.Count)
            {
                var child = await _nodeManager.GetNodeAsync(node.Children[i]);
                await foreach (var value in FindRangeInNodeAsync(child, min, max, inclusiveMin, inclusiveMax))
                {
                    yield return value;
                }
            }
        }

        /// <summary>
        /// Получает минимальный ключ
        /// </summary>
        public async ValueTask<TKey> GetMinKeyAsync()
        {
            var node = await _nodeManager.GetRootNodeAsync();
            while (!node.IsLeaf)
            {
                node = await _nodeManager.GetNodeAsync(node.Children[0]);
            }
            return node.Keys[0];
        }

        /// Получает максимальный ключ
        public async ValueTask<TKey> GetMaxKeyAsync()
        {
            var node = await _nodeManager.GetRootNodeAsync();
            while (!node.IsLeaf)
            {
                node = await _nodeManager.GetNodeAsync(node.Children[^1]);
            }
            return node.Keys[^1];
        }
    }
}
