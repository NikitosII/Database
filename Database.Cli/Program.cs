using OwnDatabase;
using Database.Core.Models;

const string DefaultDbPath = "cows.odb";

Console.WriteLine("=== OwnDatabase CLI — Cow Database ===");
Console.WriteLine($"Data file: {DefaultDbPath}");
Console.WriteLine("Type 'help' for commands, 'exit' to quit.\n");

using var db = new CowDatabase(DefaultDbPath);

while (true)
{
    Console.Write("odb> ");
    var input = Console.ReadLine()?.Trim();
    if (string.IsNullOrEmpty(input)) continue;
    if (input.Equals("exit", StringComparison.OrdinalIgnoreCase)) break;

    try
    {
        ProcessCommand(input, db);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error: {ex.Message}");
    }
}

static void ProcessCommand(string input, CowDatabase db)
{
    var parts = input.Split(' ', StringSplitOptions.RemoveEmptyEntries);
    var command = parts[0].ToLowerInvariant();

    switch (command)
    {
        case "insert":
            HandleInsert(parts, db);
            break;
        case "find":
            HandleFind(parts, db);
            break;
        case "findby":
            HandleFindBy(parts, db);
            break;
        case "update":
            HandleUpdate(parts, db);
            break;
        case "delete":
            HandleDelete(parts, db);
            break;
        case "help":
            ShowHelp();
            break;
        default:
            Console.WriteLine($"Unknown command: {command}. Type 'help' for usage.");
            break;
    }
}

static void HandleInsert(string[] parts, CowDatabase db)
{
    // insert <breed> <age> <name>
    if (parts.Length < 4)
    {
        Console.WriteLine("Usage: insert <breed> <age> <name>");
        return;
    }

    var cow = new CowModel
    {
        Id = Guid.NewGuid(),
        Breed = parts[1],
        Age = int.Parse(parts[2]),
        Name = parts[3],
        DnaData = Array.Empty<byte>()
    };

    db.Insert(cow);
    Console.WriteLine($"Inserted cow: {cow.Id} | {cow.Name} | {cow.Breed} | Age {cow.Age}");
}

static void HandleFind(string[] parts, CowDatabase db)
{
    // find <guid>
    if (parts.Length < 2)
    {
        Console.WriteLine("Usage: find <guid>");
        return;
    }

    var id = Guid.Parse(parts[1]);
    var cow = db.Find(id);
    if (cow == null)
    {
        Console.WriteLine("Not found.");
        return;
    }
    PrintCow(cow);
}

static void HandleFindBy(string[] parts, CowDatabase db)
{
    // findby <breed> <age>
    if (parts.Length < 3)
    {
        Console.WriteLine("Usage: findby <breed> <age>");
        return;
    }

    var breed = parts[1];
    var age = int.Parse(parts[2]);
    var cows = db.FindBy(breed, age).ToList();

    if (cows.Count == 0)
    {
        Console.WriteLine("No cows found.");
        return;
    }

    Console.WriteLine($"Found {cows.Count} cow(s):");
    foreach (var cow in cows)
        PrintCow(cow);
}

static void HandleUpdate(string[] parts, CowDatabase db)
{
    // update <guid> <breed> <age> <name>
    if (parts.Length < 5)
    {
        Console.WriteLine("Usage: update <guid> <breed> <age> <name>");
        return;
    }

    var cow = new CowModel
    {
        Id = Guid.Parse(parts[1]),
        Breed = parts[2],
        Age = int.Parse(parts[3]),
        Name = parts[4],
        DnaData = Array.Empty<byte>()
    };

    db.Update(cow);
    Console.WriteLine($"Updated cow {cow.Id}.");
}

static void HandleDelete(string[] parts, CowDatabase db)
{
    // delete <guid>
    if (parts.Length < 2)
    {
        Console.WriteLine("Usage: delete <guid>");
        return;
    }

    var id = Guid.Parse(parts[1]);
    var cow = db.Find(id);
    if (cow == null)
    {
        Console.WriteLine("Not found.");
        return;
    }

    db.Delete(cow);
    Console.WriteLine($"Deleted cow {id}.");
}

static void PrintCow(CowModel cow)
{
    Console.WriteLine($"  [{cow.Id}] {cow.Name} | Breed: {cow.Breed} | Age: {cow.Age} | DNA: {cow.DnaData?.Length ?? 0} bytes");
}

static void ShowHelp()
{
    Console.WriteLine("Commands:");
    Console.WriteLine("  insert <breed> <age> <name>           — Insert a new cow");
    Console.WriteLine("  find <guid>                           — Find cow by Id");
    Console.WriteLine("  findby <breed> <age>                  — Find cows by breed and age");
    Console.WriteLine("  update <guid> <breed> <age> <name>    — Update a cow");
    Console.WriteLine("  delete <guid>                         — Delete a cow");
    Console.WriteLine("  help                                  — Show this help");
    Console.WriteLine("  exit                                  — Quit");
}
