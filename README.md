# OwnDatabase

An educational database engine written in C# that stores records using custom storage, indexing, and serialization — built from scratch without any third-party database libraries.

---

## What it does

OwnDatabase stores records (Id, Breed, Age, Name, DnaData) on disk using a custom binary file format and finds them using in-memory B-tree indexes. It supports full CRUD operations and can be used via a CLI, REST API, or directly as a .NET library.

---

## Project structure

```
Database/                   - Core layer (interfaces + models)
Database.Storage/           - Storage layer (file I/O, slotted pages)
Database.Serialization/     - Serialization layer (binary format)
Database.Indexing/          - Indexing layer (B-tree)
Database.Query/             - Query layer (query engine)
OwnDatabase/                - Main library 
Database.Cli/               - Interactive command-line client
Database.Server/            - REST API 
Database.Tests/             - Unit tests
```

---

## How it works — principles

### 1. Slotted Pages (Storage layer)

#### What is a .odb file?

A `.odb` file is a raw binary file — there are no column names, no SQL, no schema stored on disk. It is a flat sequence of fixed-size **pages** (4096 bytes each). Each page stores one or more records in binary form. The file grows by one page every time the current page runs out of space.

```
cows.odb
├── Page 0  (bytes 0    – 4095)
├── Page 1  (bytes 4096 – 8191)
├── Page 2  (bytes 8192 – 12287)
└── ...
```

#### How records are stored inside a page

Each page is split into three zones. The **slot directory** grows forward from the start, the **record data** grows backward from the end, and free space sits between them:

```
┌─────────────────────────────────────────────────────┐
│ Header (8 bytes)                                    │
│   SlotCount      — how many slots exist             │
│   FreeSpacePtr   — where free space starts          │
├─────────────────────────────────────────────────────│
│ Slot 0  (4 bytes): offset + length  ──────────────┐ │
│ Slot 1  (4 bytes): offset + length  ─────────────┐│ │
│ Slot 2  (4 bytes): offset=0xFFFF   ← DELETED     ││ │
│                                                  ││ │
│              (free space)                        ││ │
│                                                  ││ │
│                 [Record 1 binary data] ←─────────┘│ │
│                      [Record 0 binary data] ←─────┘ │
└─────────────────────────────────────────────────────┘
```

- Each **slot** is a 4-byte entry: 2 bytes for the record offset, 2 bytes for its length.
- A slot with `offset = 0xFFFF` is a **tombstone** — the record was deleted. The binary data may still be physically present on the page but is invisible to the database.
- A record's position is identified by `RecordLocation(pageId, slotIndex)` — for example `(0, 1)` means page 0, slot 1.

### 2. Binary Serialization (Serialization layer)

Each cow is stored as a compact binary record:

```
[Guid: 16 bytes][Age: 4 bytes]
[Breed length: 4 bytes][Breed: N bytes UTF-8]
[Name length: 4 bytes][Name: N bytes UTF-8]
[DnaData length: 4 bytes][DnaData: N bytes]
```

Variable-length fields (Breed, Name, DnaData) are stored with a 4-byte length prefix. They are always preceded by their length, so records of different sizes can coexist on the same page.

### 3. B-Tree Indexes (Indexing layer)

Two in-memory B-trees provide O(log n) lookups:

- **Primary index** — `Guid → RecordLocation` — used by `Find(id)`
- **Secondary index** — `(Breed, Age) → RecordLocation` — used by `FindBy(breed, age)`

Both indexes are kept in memory and rebuilt by scanning all pages every time the database opens.

### 4. OwnDatabase layer

Combines all layers into the `ICowDatabase` interface:

```
ICowDatabase
  Insert(cow)              → writes to disk, updates both indexes
  Find(id)                 → primary index lookup → disk read
  FindBy(breed, age)       → secondary index lookup → disk reads
  Update(cow)              → rewrites record, updates both indexes
  Delete(cow)              → tombstone on disk, removes from indexes
```

---

## Requirements

- [.NET 9 SDK](https://dotnet.microsoft.com/download/dotnet/9.0)
- No external database libraries required

---

## See also

- [USAGE.md](USAGE.md) — detailed usage guide for CLI, API, and library
