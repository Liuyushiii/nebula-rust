fn main() {
    let heart = vec![83, 116, 111, 114, 97, 103, 101, 32, 69, 114, 114, 111, 114, 58, 32, 84, 104, 101, 32, 100, 97, 116, 97, 32, 116, 121, 112, 101, 32, 100, 111, 101, 115, 32, 110, 111, 116, 32, 109, 101, 101, 116, 32, 116, 104, 101, 32, 114, 101, 113, 117, 105, 114, 101, 109, 101, 110, 116, 115, 46, 32, 85, 115, 101, 32, 116, 104, 101, 32, 99, 111, 114, 114, 101, 99, 116, 32, 116, 121, 112, 101, 32, 111, 102, 32, 100, 97, 116, 97, 46];
    let heart = String::from_utf8(heart).unwrap();
    println!("{heart:?}");
}