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