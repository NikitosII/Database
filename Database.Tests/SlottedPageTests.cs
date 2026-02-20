using Xunit;
using Database.Storage;

namespace Database.Tests;

public class SlottedPageTests
{
    [Fact]
    public void Insert_And_Get_SingleRecord()
    {
        var page = new SlottedPage(4096);
        var data = new byte[] { 1, 2, 3, 4, 5 };

        int slot = page.Insert(data);

        Assert.Equal(0, slot);
        Assert.Equal(data, page.Get(slot));
    }

    [Fact]
    public void Insert_MultipleRecords_AllRetrievable()
    {
        var page = new SlottedPage(4096);
        var records = new List<byte[]>();

        for (int i = 0; i < 10; i++)
        {
            var record = new byte[] { (byte)i, (byte)(i + 1), (byte)(i + 2) };
            records.Add(record);
            page.Insert(record);
        }

        for (int i = 0; i < 10; i++)
        {
            Assert.Equal(records[i], page.Get(i));
        }
    }

    [Fact]
    public void Delete_Slot_ReturnsNull()
    {
        var page = new SlottedPage(4096);
        page.Insert(new byte[] { 1, 2, 3 });
        page.Insert(new byte[] { 4, 5, 6 });

        page.Delete(0);

        Assert.Null(page.Get(0));
        Assert.Equal(new byte[] { 4, 5, 6 }, page.Get(1));
    }

    [Fact]
    public void Insert_PageFull_ReturnsMinusOne()
    {
        var page = new SlottedPage(64); // Tiny page
        var bigRecord = new byte[40];

        int slot = page.Insert(bigRecord);
        Assert.Equal(0, slot);

        // Second record should not fit
        int slot2 = page.Insert(bigRecord);
        Assert.Equal(-1, slot2);
    }

    [Fact]
    public void EnumerateRecords_SkipsDeleted()
    {
        var page = new SlottedPage(4096);
        page.Insert(new byte[] { 1 });
        page.Insert(new byte[] { 2 });
        page.Insert(new byte[] { 3 });

        page.Delete(1);

        var records = page.EnumerateRecords().ToList();
        Assert.Equal(2, records.Count);
        Assert.Equal(0, records[0].SlotIndex);
        Assert.Equal(2, records[1].SlotIndex);
    }

    [Fact]
    public void RawData_PreservesPage_WhenReloaded()
    {
        var page = new SlottedPage(4096);
        page.Insert(new byte[] { 10, 20, 30 });
        page.Insert(new byte[] { 40, 50 });

        // Simulate writing to disk and reading back
        var raw = page.GetRawData();
        var restored = new SlottedPage((byte[])raw.Clone());

        Assert.Equal(new byte[] { 10, 20, 30 }, restored.Get(0));
        Assert.Equal(new byte[] { 40, 50 }, restored.Get(1));
    }
}
