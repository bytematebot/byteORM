<img src="./assets/banner.png" alt="ByteORM Banner" />

---

[![Crates.io](https://img.shields.io/badge/crates.io-0.1.6-orange)](https://crates.io/crates/byteorm)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> ByteORM is a lightweight ORM for Rust that generates a fully typed client crate from a Prisma-like `.bo` schema. Define your schema, run `byteorm push`, and use a typed Rust client with query builders, CRUD operations, and connection pooling.

## Using ByteORM

### 1. Initialize

```bash
byteorm init
```

This creates `byteorm.toml`, a starter `schema.bo`, and a generated-client ignore entry. Existing projects that already use `byteorm/*.bo` or `generated/` keep that layout.

Example `byteorm.toml` for a single schema:

```toml
[schema]
path = "schema.bo"

[client]
output = ".byteorm/client"
crate_name = "byteorm-client"
dependency_source = "vendored"
```

Example for multi-schema projects:

```toml
[schema]
directory = "byteorm"

[client]
output = "generated"
crate_name = "byteorm-client"
dependency_source = "vendored"
```

ByteORM v0.1.x keeps `dependency_source = "vendored"` as the default. The generated client includes its matching `byteorm-macros` copy, so projects do not need separate crates.io packages yet.

### 2. Define Your Schema

Create or edit `schema.bo`:

```bo
enum PostStatus {
    Draft
    Published
    Archived
}

model User {
    id         BigInt      PrimaryKey
    email      String      Unique
    username   String      NotNull
    created_at TimestamptZ @default(now())
}

model Post {
    id         BigInt      PrimaryKey
    user_id    BigInt      NotNull ForeignKey(User.id, onDelete: cascade)
    title      String      NotNull
    content    String      NotNull
    status     PostStatus  @default(Draft)
    created_at TimestamptZ @default(now()) Index
    updated_at TimestamptZ @default(now())
}
```

### 3. Generate Or Push

Generate the typed client without touching the database:

```bash
byteorm generate
```

Push schema changes to the database and regenerate the client:

```bash
byteorm push
```

If a migration would drop tables, columns, or enum types, ByteORM stops unless you explicitly accept it:

```bash
byteorm push --accept-data-loss
```

Inspect resolved paths and config:

```bash
byteorm doctor
```

Generate shell autocomplete scripts:

```bash
byteorm completions
byteorm completions install
byteorm completions powershell
byteorm completions zsh
byteorm completions bash
```

`byteorm completions` detects the current shell and prints the recommended install command. `byteorm completions install` writes the completion script to the standard location for bash, zsh, fish, PowerShell, or elvish.

For a one-off bash session without installing anything:

```bash
source <(byteorm completions bash)
```

The install scripts also detect the current shell and ask whether to install autocomplete automatically. Set `BYTEORM_INSTALL_COMPLETIONS=1` to install without prompting, or `BYTEORM_INSTALL_COMPLETIONS=0` to skip it.

### 4. Use The Client

Add the generated client path printed by `byteorm init` or `byteorm push` to your `Cargo.toml`:

```toml
[dependencies]
byteorm-client = { path = ".byteorm/client" }
```

Then use it from Rust:

```rust
use byteorm_client::{Client, PostStatus};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = Client::new("postgres://user:pass@localhost:5432/mydb").await?;

    let user = client.user.create(|u| {
        u.set_email("alice@example.com")
            .set_username("alice")
    }).await?;

    let post = client.post.create(|p| {
        p.set_user_id(user.id)
            .set_title("Hello ByteORM")
            .set_content("This is my first post using ByteORM!")
            .set_status(PostStatus::Draft)
    }).await?;

    println!("Created post: {} (id={})", post.title, post.id);

    Ok(())
}
```

> A complete working example lives in [`examples/blog/`](examples/blog/). Run it with `cargo run --example blog` from that directory.
