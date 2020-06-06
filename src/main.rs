use std::fs::File;  
use std::io::Read;
use std::sync::Arc;  
use arrow::csv;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::Result; 
use arrow::util::pretty::print_batches;                                                                                                                                                                                   
use std::vec::Vec;

use wasmer_runtime::{imports, instantiate, Func, Array, WasmPtr};

use std::time::{Instant};

fn main() -> Result<()> {

    let schema = Schema::new(vec![
        Field::new("city", DataType::Utf8, false),
        Field::new("lat", DataType::Float64, false),
        Field::new("lng", DataType::Float64, false),
    ]);

    let file = File::open("uk_cities.csv").unwrap();

    let mut csv = csv::Reader::new(file, Arc::new(schema), false, 1024, None);
    //let batch = csv.next().unwrap().unwrap();
/*
    let mut writer  = Vec::<u8>::new();

    let schema = Schema::new(vec![
        Field::new("city", DataType::Utf8, false),
        Field::new("lat", DataType::Float64, false),
        Field::new("lng", DataType::Float64, false),
    ]);

   {
    let mut w = arrow::ipc::writer::StreamWriter::try_new(& mut writer, &schema).unwrap();
    w.write(&batch).unwrap();
    w.finish().unwrap();
   }

    let mut reader = arrow::ipc::reader::StreamReader::try_new(writer.as_slice()).unwrap();

    print!("Value {}\n", call_to_wasm(&writer));


    print!("Schema {}\n", batch.schema().to_json());
    */


    let schema = Schema::new(vec![
        Field::new("city", DataType::Utf8, false),
        Field::new("lat", DataType::Float64, false),
        Field::new("lng", DataType::Float64, false),
    ]);

    let mut writer = File::create("simple.arrow").unwrap();

    let mut w = arrow::ipc::writer::FileWriter::try_new(& mut writer, &schema).unwrap();
    while let Ok(Some(batch)) = csv.next() {
      w.write(&batch).unwrap();
    }
    //w.write(&batch).unwrap();
    w.finish()
 

   //print_batches(&vec![reader.next().unwrap().unwrap()])

}

fn call_to_wasm(data: &Vec<u8>) -> u32 {
    let mut f = File::open("add.wasm").unwrap();
    let mut buffer = Vec::new();

    f.read_to_end(&mut buffer).unwrap();     

    // Our import object, that allows exposing functions to our Wasm module.
    // We're not importing anything, so make an empty import object.
    let import_object = imports! {};

    let t1 = Instant::now();
    // Let's create an instance of Wasm module running in the wasmer-runtime
    let instance = instantiate(&buffer, &import_object).unwrap();

    print!("Compile time: {} ms\n", Instant::now().duration_since(t1).as_millis());

    //let malloc: Func<u32, WasmPtr<u32, Array>> = instance.func("__wbindgen_malloc")?;
    let malloc: Func<u32, WasmPtr<u8,Array>> = instance.func("__wbindgen_malloc").unwrap();


    let t1 = Instant::now();
    let wasm_ptr = malloc.call(data.len() as u32).unwrap();
    print!("Malloc time: {} ms\n", Instant::now().duration_since(t1).as_millis());


    unsafe {
       let t1 = Instant::now();
       let wasm_array = wasm_ptr.deref_mut(instance.context().memory(0),0,data.len() as u32).unwrap();
       for idx in 0..data.len() {
         wasm_array[idx].set(data[idx]);
       }
       print!("Copy time: {} ms\n", Instant::now().duration_since(t1).as_millis());
    }

    let t1 = Instant::now();
    let reduce: Func<(WasmPtr<u8,Array>, u32), u32> =  instance.func("reduce").unwrap();
    let result = reduce.call(wasm_ptr, data.len() as u32).unwrap();
    print!("Call time: {} ms\n", Instant::now().duration_since(t1).as_millis());



    result
}
