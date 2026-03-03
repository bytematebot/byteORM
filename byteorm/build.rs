use std::env;
use std::fs;
use std::io::Write;
use std::path::Path;

fn main() {
    let macros_dir = Path::new("../byteorm-macros");
    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("embedded_macros.rs");
    let mut f = fs::File::create(&dest_path).unwrap();

    writeln!(f, "pub fn embedded_macros_files() -> Vec<(&'static str, &'static str)> {{").unwrap();
    writeln!(f, "    vec![").unwrap();

    embed_dir(&mut f, macros_dir, macros_dir);

    writeln!(f, "    ]").unwrap();
    writeln!(f, "}}").unwrap();

    println!("cargo:rerun-if-changed=../byteorm-macros");
}

fn embed_dir(f: &mut fs::File, base: &Path, dir: &Path) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().map(|n| n == "target").unwrap_or(false) {
                    continue;
                }
                embed_dir(f, base, &path);
            } else {
                let rel = path.strip_prefix(base).unwrap();
                let rel_str = rel.display().to_string().replace('\\', "/");
                let abs = path.canonicalize().unwrap();
                let abs_str = abs.display().to_string().replace('\\', "/");
                let abs_clean = abs_str.strip_prefix("//?/").unwrap_or(&abs_str);
                writeln!(
                    f,
                    "        (\"{}\", include_str!(\"{}\")),",
                    rel_str, abs_clean
                )
                .unwrap();
            }
        }
    }
}
