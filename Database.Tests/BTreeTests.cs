using Xunit;
using Database.Core.Models;
using Database.Indexing;

namespace Database.Tests;

public class BTreeTests
{
    private BTree<int, string> CreateTree(int degree = 3)
    {
        var nodeManager = new InMemoryNodeManager<int, string>();
        return new BTree<int, string>("test", nodeManager, degree);
    }

    [Fact]
    public void Insert_And_Find_SingleKey()
    {
        var tree = CreateTree();
        tree.Insert(42, "hello");

        var results = tree.Find(42).ToList();

        Assert.Single(results);
        Assert.Equal("hello", results[0]);
    }

    [Fact]
    public void Insert_ManyKeys_AllFound()
    {
        var tree = CreateTree();
        for (int i = 0; i < 100; i++)
            tree.Insert(i, $"value_{i}");

        for (int i = 0; i < 100; i++)
        {
            var results = tree.Find(i).ToList();
            Assert.Single(results);
            Assert.Equal($"value_{i}", results[0]);
        }
    }

    [Fact]
    public void Delete_Key_NotFoundAfter()
    {
        var tree = CreateTree();
        tree.Insert(1, "a");
        tree.Insert(2, "b");
        tree.Insert(3, "c");

        bool deleted = tree.Delete(2, "b");

        Assert.True(deleted);
        Assert.Empty(tree.Find(2));
        Assert.Single(tree.Find(1));
        Assert.Single(tree.Find(3));
    }

    [Fact]
    public void Delete_NonExistentKey_ReturnsFalse()
    {
        var tree = CreateTree();
        tree.Insert(1, "a");

        bool deleted = tree.Delete(99, "a");

        Assert.False(deleted);
    }

    [Fact]
    public void FindRange_ReturnsCorrectValues()
    {
        var tree = CreateTree();
        for (int i = 1; i <= 20; i++)
            tree.Insert(i, $"v{i}");

        var range = tree.FindRange(5, 10).ToList();

        Assert.Equal(6, range.Count);
        Assert.Contains("v5", range);
        Assert.Contains("v10", range);
        Assert.DoesNotContain("v4", range);
        Assert.DoesNotContain("v11", range);
    }

    [Fact]
    public void GetMinKey_ReturnsSmallest()
    {
        var tree = CreateTree();
        tree.Insert(50, "a");
        tree.Insert(10, "b");
        tree.Insert(90, "c");
        tree.Insert(5, "d");

        Assert.Equal(5, tree.GetMinKey());
    }

    [Fact]
    public void GetMaxKey_ReturnsLargest()
    {
        var tree = CreateTree();
        tree.Insert(50, "a");
        tree.Insert(10, "b");
        tree.Insert(90, "c");
        tree.Insert(5, "d");

        Assert.Equal(90, tree.GetMaxKey());
    }

    [Fact]
    public void Insert_CausesSplits_TreeRemainsCorrect()
    {
        // degree=2 means max 3 keys per node — lots of splits
        var tree = CreateTree(degree: 2);
        for (int i = 0; i < 50; i++)
            tree.Insert(i, $"v{i}");

        for (int i = 0; i < 50; i++)
            Assert.Single(tree.Find(i));
    }

    [Fact]
    public void Delete_ManyKeys_TreeRemainsCorrect()
    {
        var tree = CreateTree(degree: 2);
        for (int i = 0; i < 30; i++)
            tree.Insert(i, $"v{i}");

        // Delete even numbers
        for (int i = 0; i < 30; i += 2)
            tree.Delete(i, $"v{i}");

        // Odd numbers still present
        for (int i = 1; i < 30; i += 2)
            Assert.Single(tree.Find(i));

        // Even numbers gone
        for (int i = 0; i < 30; i += 2)
            Assert.Empty(tree.Find(i));
    }
}
