mod transcoder;

use transcoder::transcoder;

fn main() -> Result<(), String> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: {} <input> <output>", args[0]);
        std::process::exit(1);
    }

    transcoder(&args[1], &args[2], "")?;
    println!("Transcoding complete: {} -> {}", &args[1], &args[2]);
    Ok(())
}
