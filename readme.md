<img src="./assets/banner.png" alt="ByteORM Banner" />

---


[![Crates.io](https://img.shields.io/badge/crates.io-0.1.6-orange)](https://crates.io/crates/byteorm)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)


> Byteorm is a lightweight ORM for Rust that generates a fully typed client crate from a simple prisma-like schema definition.  Write your database schema in a clean .bo DSL file, then run
byteorm push to apply migrations and generate a Rust client with query builders, CRUD operations, and connection pooling built in.


## Using ByteORM

### 1. Define your schema

Create a `.bo` file — for example `schema.bo`:

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

### 2. Push to database

```bash
byteorm push
```

This generates a `byteorm-client` crate in `generated/` with fully typed query builders.

### 3. Write your application

```rust
use byteorm_client::{Client, PostStatus};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = Client::new("postgres://user:pass@localhost:5432/mydb").await?;

    let user = client.user.create(|u| {
        u.set_email("alice@example.com")
            .set_username("alice")
    }).await?;

    println!("Created user: {} (id={})", user.username, user.id);

    let post = client.post.create(|p| {
        p.set_user_id(user.id)
            .set_title("Hello ByteORM")
            .set_content("This is my first post using ByteORM!")
    }).await?;

    println!("Created post: {} (id={})", post.title, post.id);

    let posts = client.post.find_many(|p| {
        p.where_user_id(user.id)
            .order_by_created_at_desc()
    }).await?;

    println!("User has {} post(s)", posts.len());

    client.post.update(|p| {
        p.where_id(post.id)
            .set_status(PostStatus::Published)
    }).await?;

    println!("Post status updated to Published");

    Ok(())
}
```

### 4. Run it

```bash
cargo run
```

> A complete working example lives in [`examples/blog/`](examples/blog/).