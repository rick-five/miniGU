// test_minigu_lib.rs - A simple test to verify minigu library behavior on macOS
// This can be run with: rustc --extern minigu=target/debug/libminigu.rlib test_minigu_lib.rs && ./test_minigu_lib

use minigu::database::{Database, DatabaseConfig};
use minigu::session::Session;

fn main() {
    println!("Attempting to create DatabaseConfig...");
    let config = DatabaseConfig::default();
    println!("DatabaseConfig created.");

    println!("Attempting to open_in_memory Database...");
    match Database::open_in_memory(&config) {
        Ok(db) => {
            println!("Database opened successfully.");
            
            // Try creating a session too
            println!("Attempting to create Session...");
            match db.session() {
                Ok(_session) => {
                    println!("Session created successfully.");
                    println!("All good!");
                },
                Err(e) => {
                    eprintln!("Failed to create session: {}", e);
                }
            }
        },
        Err(e) => {
            eprintln!("Failed to open database: {}", e);
        }
    }
}