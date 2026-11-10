// A normal native command shim. No shell parsing or dependency on the source tree.
use std::{
    env, fs,
    process::{exit, Command},
};

#[cfg(windows)]
#[link(name = "kernel32")]
unsafe extern "system" {
    fn SetConsoleCtrlHandler(handler: Option<extern "system" fn(u32) -> i32>, add: i32) -> i32;
}

#[cfg(windows)]
extern "system" fn wait_for_product(event: u32) -> i32 {
    // Ctrl-C / Ctrl-Break also reach the product in our console process group.
    // Keep this wrapper alive to return the product's eventual exit status.
    i32::from(event <= 1)
}

fn main() {
    #[cfg(windows)]
    unsafe {
        SetConsoleCtrlHandler(Some(wait_for_product), 1);
    }
    let result = (|| -> Result<i32, Box<dyn std::error::Error>> {
        let path = env::current_exe()?.with_extension("aixand-command");
        let record = fs::read_to_string(path)?;
        let lines: Vec<_> = record.lines().collect();
        if lines.len() != 5 || lines[0] != "aixand-command/2" {
            return Err("Invalid installed command descriptor".into());
        }
        let status = Command::new(lines[1])
            .args([
                "-I",
                "-B",
                lines[2],
                "exec",
                "--prefix",
                lines[3],
                "--command",
                lines[4],
                "--",
            ])
            .args(env::args_os().skip(1))
            .status()?;
        Ok(status.code().unwrap_or(130))
    })();
    match result {
        Ok(code) => exit(code),
        Err(error) => {
            eprintln!("error: {error}");
            exit(1);
        }
    }
}
