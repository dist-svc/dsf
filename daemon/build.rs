use std::process::Command;

fn main() {
    // Rebuild on build.rs changes

    // Fetch git describe version
    let output = Command::new("git")
        .args(["describe", "--dirty=+", "--always"])
        .output()
        .expect("git describe failed");

    let version = std::str::from_utf8(&output.stdout)
        .unwrap()
        .trim()
        .to_string();

    // Write version to variable for rustc
    println!("cargo:rustc-env=GIT_TAG={version}");
}
