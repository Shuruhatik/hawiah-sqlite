# @hawiah/sqlite [![NPM version](https://img.shields.io/npm/v/@hawiah/sqlite.svg?style=flat-square&color=informational)](https://npmjs.com/package/@hawiah/sqlite)

SQLite driver for Hawiah - lightweight and fast data persistence with better-sqlite3.

Now supports **Hybrid Storage**: Use it as a pure key-value NoSQL store OR define a Schema for high-performance SQL columns mixed with flexible JSON storage.

## Installation

```bash
npm install @hawiah/sqlite
```

## Usage

### 1. Default Mode (NoSQL / Schema-less)
By default, the driver behaves like a NoSQL document store. All data is stored in a single JSON blob column. This offers maximum flexibility.

```typescript
import { SQLiteDriver } from '@hawiah/sqlite';

const driver = new SQLiteDriver('./mydb.sqlite', 'users');
await driver.connect();

// Store arbitrary data
await driver.set({ name: 'Ali', age: 25, role: 'admin' });
```

### 2. Hybrid Mode (Schema / Real SQL)
**New in v0.2.x**: You can provide a schema using `setSchema(schema)`. The driver will create **Real SQL Columns** for the schema fields (improving query performance and storage efficiency) while keeping a specialized `_extras` JSON column for any extra dynamic fields.

This gives you the "Best of Both Worlds":
- **Structure & Speed** for known fields (SQL).
- **Flexibility** for unknown/runtime fields (NoSQL).

```typescript
import { SQLiteDriver } from '@hawiah/sqlite';

const driver = new SQLiteDriver('./mydb.sqlite', 'products');

// Define Schema (Optional)
// Hawiah types are mapped to SQLite types (e.g., STRING -> TEXT, NUMBER -> REAL)
driver.setSchema({
    title: { type: 'STRING' },
    price: { type: 'NUMBER' },
    inStock: { type: 'BOOLEAN' }
});

await driver.connect();

// Inserting data:
// 'title', 'price', 'inStock' go into real SQL columns.
// 'tags', 'meta' go into the '_extras' JSON column automatically.
await driver.set({ 
    title: 'Gaming Mouse', 
    price: 150, 
    inStock: true,
    tags: ['wireless', 'rgb'], // stored in _extras
    meta: { supplier: 'X' }    // stored in _extras
});

// Reading data:
// The driver automatically merges SQL columns and _extras back into a single object.
const product = await driver.getOne({ title: 'Gaming Mouse' });
console.log(product.tags); // ['wireless', 'rgb']
```

## Features

- **Fast & Reliable**: Built on `better-sqlite3` (synchronous API for max speed).
- **Hybrid Storage**: Choose between Schema-less (JSON blob) or Hybrid (SQL + JSON).
- **Adaptive**: Automatically maps Schema types to SQLite (TEXT, INTEGER, REAL, BLOB).
- **Transactions**: Full support for `beginTransaction`, `commit`, `rollback`.
- **Automatic Indexing**: Default indexes on `_createdAt` and `_updatedAt`.
- **Developer Friendly**: Simple `IDriver` interface from Hawiah Core.

## License

MIT