use std::sync::Mutex;

fn main(){

    let a = Mutex::new(5 as usize);
        
    let t1: usize;

    {
        let a1 = a.lock().unwrap();
        t1 = *a1;
    }

    println!("{:?}", t1);
    let a2 = a.lock().unwrap();




}