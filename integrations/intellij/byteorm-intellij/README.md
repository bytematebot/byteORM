# ByteORM IntelliJ Support

<!-- Plugin description -->

ByteORM IntelliJ Support provides basic `.bo` language support for ByteORM schema files:

- file type registration for `.bo`
- ByteORM syntax highlighting
- context-aware keyword, type, model, and enum completion
- editor validation for common schema errors
- automatic block formatting after `{` + Enter

This is the MVP layer only. It is intentionally small so it can later be extracted into a standalone editor-support repository or mirrored for VS Code and Zed.

<!-- Plugin description end -->

## Layout

- `src/main/kotlin` contains the language/file type, lexer, highlighting, completion, and symbol index.
- `src/main/resources/META-INF/plugin.xml` registers the IntelliJ extensions.

## Notes

- This project is kept as a normal subproject inside the ByteORM repository, not a git submodule.
- That keeps the editor-support code easy to evolve alongside ByteORM while still leaving a clean boundary for later extraction.

## Run

From this directory:

```powershell
.\gradlew.bat runIde
```

That launches a sandbox IntelliJ IDEA instance with the ByteORM plugin loaded.

## Release

The repository workflow builds this plugin only when files under `integrations/intellij/byteorm-intellij/` change.
CI uses `pluginVersion` from `gradle.properties`, uploads the ZIP as a workflow artifact, and publishes to JetBrains Marketplace on `main` when `JETBRAINS_MARKETPLACE_TOKEN` is configured.
Pushes to `main` skip plugin build and publish steps unless `pluginVersion` changed since the previous commit. Bump `pluginVersion` when a plugin update should be published.

Optional signing secrets, if plugin signing is needed:

- `JETBRAINS_CERTIFICATE_CHAIN`
- `JETBRAINS_PRIVATE_KEY`
- `JETBRAINS_PRIVATE_KEY_PASSWORD`
