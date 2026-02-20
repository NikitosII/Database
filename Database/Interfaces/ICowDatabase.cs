using Database.Core.Models;

namespace Database.Core.Interfaces
{
    public interface ICowDatabase : IDisposable
    {
        void Insert(CowModel cow);
        void Delete(CowModel cow);
        void Update(CowModel cow);
        CowModel? Find(Guid id);
        IEnumerable<CowModel> FindBy(string breed, int age);
    }
}
