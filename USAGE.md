# USAGE — OwnDatabase

## Table of Contents
- [Using OwnDatabase as a library](#using-owndatabase-as-a-library)
- [Interactive command line (CLI)](#interactive-command-line-cli)
- [REST API](#rest-api)
- [Running tests](#running-tests)
- [Building](#building)

---

## Using OwnDatabase as a library

Add a reference to `OwnDatabase.csproj` and use `CowDatabase` directly:

```csharp
using OwnDatabase;
using Database.Core.Models;

// Open (or create) a database file
using var db = new CowDatabase("myfile.odb");

// Insert
var cow = new CowModel
{
    Id      = Guid.NewGuid(),
    Breed   = "Holstein",
    Age     = 3,
    Name    = "Bessie",
    DnaData = new byte[] { 0x41, 0x43, 0x47, 0x54 }
};
db.Insert(cow);

// Find by Id
CowModel? found = db.Find(cow.Id);

// Find by breed and age
IEnumerable<CowModel> results = db.FindBy("Holstein", 3);

// Update
cow.Name = "Bessie II";
db.Update(cow);

// Delete
db.Delete(cow);
```

The `using` statement ensures the file is properly closed and flushed when done.

---

## Interactive command line (CLI)

```bash
dotnet run --project Database.Cli
```

You will see the prompt:

```
=== OwnDatabase CLI — Cow Database ===
Data file: cows.odb
Type 'help' for commands, 'exit' to quit.

odb>
```

### Available commands

| Command | Description | Example |
|---|---|---|
| `insert <breed> <age> <name>` | Insert a new cow | `insert Holstein 3 Bessie` |
| `find <guid>` | Find a cow by Id | `find 3f2504e0...` |
| `findby <breed> <age>` | Find cows by breed and age | `findby Holstein 3` |
| `update <guid> <breed> <age> <name>` | Update a cow | `update 3f25... Jersey 4 Molly` |
| `delete <guid>` | Delete a cow | `delete 3f25...` |
| `help` | Show all commands | `help` |
| `exit` | Quit the CLI | `exit` |

### Example session

```
odb> insert Holstein 3 Bessie
Inserted cow: 5bc09e6b-309a-4448-8e1c-ffd028500396 | Bessie | Holstein | Age 3

odb> insert Holstein 3 Daisy
Inserted cow: 07d2d687-8bfa-46b7-834c-96952f5a13b1 | Daisy | Holstein | Age 3

odb> findby Holstein 3
Found 2 cow(s):
  [5bc09e6b-309a-4448-8e1c-ffd028500396] Bessie | Breed: Holstein | Age: 3 | DNA: 0 bytes
  [07d2d687-8bfa-46b7-834c-96952f5a13b1] Daisy | Breed: Holstein | Age: 3 | DNA: 0 bytes

odb> find 5bc09e6b-309a-4448-8e1c-ffd028500396
  [5bc09e6b-309a-4448-8e1c-ffd028500396] Bessie | Breed: Holstein | Age: 3 | DNA: 0 bytes

odb> update 5bc09e6b-309a-4448-8e1c-ffd028500396 Jersey 4 BessieJr
Updated cow 5bc09e6b-309a-4448-8e1c-ffd028500396.

odb> delete 5bc09e6b-309a-4448-8e1c-ffd028500396
Deleted cow 5bc09e6b-309a-4448-8e1c-ffd028500396.

odb> exit
```

Data is persisted to `cows.odb` in the working directory. The file is kept between sessions.

---

## REST API

```bash
dotnet run --project Database.Server
# Listening on http://l ocalhost:5000
```

### Endpoints

#### Insert a cow
```http
POST /api/cows
Content-Type: application/json

{
  "breed": "Holstein",
  "age": 3,
  "name": "Bessie",
  "dnaData": null
}
```
Response: `201 Created` with the new cow object.

#### Find by Id
```http
GET /api/cows/3f2504e0-4f89-11d3-9a0c-0305e82c3301
```
Response: `200 OK` with the cow, or `404 Not Found`.

#### Find by breed and age
```http
GET /api/cows/search?breed=Holstein&age=3
```
Response: `200 OK` with an array of matching cows.

#### Update a cow
```http
PUT /api/cows
Content-Type: application/json

{
  "id": "3f2504e0-4f89-11d3-9a0c-0305e82c3301",
  "breed": "Jersey",
  "age": 4,
  "name": "BessieJr",
  "dnaData": null
}
```
Response: `200 OK`.

#### Delete a cow
```http
DELETE /api/cows/3f2504e0-4f89-11d3-9a0c-0305e82c3301
```
Response: `204 No Content`, or `404 Not Found`.

### Testing the API with curl

```bash
# Insert
curl -X POST http://localhost:5000/api/cows \
  -H "Content-Type: application/json" \
  -d '{"breed":"Holstein","age":3,"name":"Bessie"}'

# Find by Id (replace with actual guid from insert response)
curl http://localhost:5000/api/cows/3f2504e0-4f89-11d3-9a0c-0305e82c3301

# Search by breed and age
curl "http://localhost:5000/api/cows/search?breed=Holstein&age=3"

# Delete
curl -X DELETE http://localhost:5000/api/cows/3f2504e0-4f89-11d3-9a0c-0305e82c3301
```

---

## Running tests

```bash
dotnet test
```

Expected output:

```
Running tests...

✅ PASSED: Delete_Slot_ReturnsNull 
✅ PASSED: Insert_MultipleRecords_AllRetrievable 
✅ PASSED: Serialize_Deserialize_RoundTrip 
✅ PASSED: Insert_PageFull_ReturnsMinusOne 
✅ PASSED: Serialize_UnicodeStrings_Works 
✅ PASSED: Insert_LargeDnaData_Works 
✅ PASSED: Delete_Key_NotFoundAfter 
✅ PASSED: GetMinKey_ReturnsSmallest 
✅ PASSED: Insert_And_Find_SingleKey 
✅ PASSED: Serialize_EmptyStrings_Works 
✅ PASSED: Serialize_LargeDnaData_Works 
✅ PASSED: EnumerateRecords_SkipsDeleted 
✅ PASSED: RawData_PreservesPage_WhenReloaded 
✅ PASSED: Insert_And_Get_SingleRecord 
✅ PASSED: Delete_ManyKeys_TreeRemainsCorrect 
✅ PASSED: Insert_ManyKeys_AllFound 
✅ PASSED: GetMaxKey_ReturnsLargest 
✅ PASSED: Delete_NonExistentKey_ReturnsFalse 
✅ PASSED: Insert_CausesSplits_TreeRemainsCorrect 
✅ PASSED: FindBy_BreedAndAge 
✅ PASSED: FindRange_ReturnsCorrectValues 
✅ PASSED: Insert_DuplicateId_Throws 
✅ PASSED: Persistence_DataSurvivesReopen 
✅ PASSED: Update_ChangesSecondaryIndex 
✅ PASSED: Persistence_FindBy_WorksAfterReopen 
✅ PASSED: Delete_RemovesCow 
✅ PASSED: Find_NonExistent_ReturnsNull 
✅ PASSED: Update_ChangesCow 
✅ PASSED: Insert_ManyCows_AllRetrievable 
✅ PASSED: Insert_And_Find_ById 

========================================
Test Run Summary
========================================
Total Tests: 30
Passed: 30
Failed: 0
Skipped: 0
Result: ✅ SUCCEEDED
Test Execution Time: 0.16s (162ms)
Total Time (including build): 8.14s (8140ms)
========================================
```

### Test coverage

| Test class | What it tests |
|---|---|
| `CowSerializerTests` | Binary serialization round-trips, Unicode, empty fields, large DNA |
| `SlottedPageTests` | Insert/read, multiple records, deletion, page-full detection, persistence |
| `BTreeTests` | Insert, delete, range search, duplicate keys, tree splits (degree=2) |
| `CowDatabaseTests` | Full CRUD, persistence across reopen, secondary index, 100+ records |

### Run a specific test class

```bash
dotnet test --filter "ClassName=Database.Tests.BTreeTests"
dotnet test --filter "ClassName=Database.Tests.CowDatabaseTests"
dotnet test --filter "ClassName=Database.Tests.SlottedPageTests"
dotnet test --filter "ClassName=Database.Tests.CowSerializerTests"
```

### Run a single test

```bash
dotnet test --filter "FullyQualifiedName=Database.Tests.BTreeTests.Insert_CausesSplits_TreeRemainsCorrect"
```

---

## Building

```bash
# Build entire solution
dotnet build

# Build a specific project
dotnet build Database.Storage/Database.Storage.csproj
dotnet build OwnDatabase/OwnDatabase.csproj

```
