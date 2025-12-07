use byteorm_lib::*;

fn main() {
    let schema_text = r#"
enum UserStatus {
    ONLINE
    DND
    IDLE
    LOOKING_TO_PLAY
    UNAVAILABLE
}

enum Theme {
    LIGHT
    DARK
    DIM
}

enum MessageType {
    DEFAULT
    USER_JOIN
    USER_LEAVE
}

model Users {
    id          Text        PrimaryKey
    username    Text        NotNull
    tag         Text        NotNull
    created_at  TimestamptZ NotNull     @default(now())
    bot         Boolean     NotNull     @default(false)
    status      UserStatus  NotNull     @default(ONLINE)
    flags       Int         NotNull     @default(0)
    bio         Text?
    avatar      Text?
    banner      Text?
}

model Accounts {
    id              Text        PrimaryKey
    email           Text        Unique NotNull
    password        Text        NotNull
    user_id         Text        NotNull     ForeignKey(Users.id, onDelete: cascade)
    email_verified  Boolean     NotNull     @default(false)
    locale          Text        NotNull     @default(en_us)
    token           Text        Unique NotNull
}

model Guilds {
    id          Text        PrimaryKey
    name        Text        NotNull     @default(New Server)
    brief       Text        NotNull     @default(A server to talk)
    icon        Text?
    created_at  TimestamptZ @default(now())
    owner_id    Text        NotNull
}

model GuildMembers {
    id          Serial      PrimaryKey
    guild_id    Text        NotNull     ForeignKey(Guilds.id, onDelete: cascade)
    user_id     Text        NotNull     ForeignKey(Users.id, onDelete: cascade)
    nickname    Text?
    joined_at   TimestamptZ NotNull     @default(now())
}

model Channels {
    id                  Text        PrimaryKey
    name                Text        NotNull     @default(General)
    guild_id            Text        NotNull     ForeignKey(Guilds.id, onDelete: cascade)
    created_at          TimestamptZ @default(now())
    rate_limit_per_user Int         NotNull     @default(0)
}

model Messages {
    id          Text        PrimaryKey
    author_id   Text        NotNull     ForeignKey(Users.id, onDelete: no action)
    channel_id  Text        NotNull     ForeignKey(Channels.id, onDelete: cascade)
    guild_id    Text        NotNull     ForeignKey(Guilds.id, onDelete: cascade)
    content     Text?
    created_at  TimestamptZ @default(now())
    updated_at  TimestamptZ?
    type        MessageType @default(DEFAULT)
    nonce       Text        @default(0)
}
"#;

    let schema = parse_schema(schema_text).expect("Failed to parse schema");
    
    println!("Parsed {} enums, {} models", schema.enums.len(), schema.models.len());
    
    println!("\n=== ENUMS ===");
    for enum_def in &schema.enums {
        println!("\nEnum: {}", enum_def.name);
        for value in &enum_def.values {
            println!("  - {}", value);
        }
    }
    
    println!("\n=== MODELS ===");
    for model in &schema.models {
        println!("\nModel: {}", model.name);
        for field in &model.fields {
            let attrs_str = if !field.attributes.is_empty() {
                format!(" [{}]", field.attributes.iter()
                    .map(|a| format!("@{}", a.name))
                    .collect::<Vec<_>>()
                    .join(", "))
            } else {
                String::new()
            };
            println!("  {} : {}{}", field.name, field.type_name, attrs_str);
        }
    }
    
    println!("\n=== GENERATED SQL ===");
    let changes = diff::diff_schemas(None, &schema);
    let sql = codegen::generate_migration_sql(&changes);
    
    for line in sql.split(';') {
        if !line.trim().is_empty() {
            println!("{};", line.trim());
        }
    }
}
