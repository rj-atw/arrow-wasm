use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn readme() -> String { 
  return String::from("Calculates the variance")
}

#[wasm_bindgen]
pub fn reduce(a: &[u8]) -> u32 { 
  let mut reader = arrow::ipc::reader::StreamReader::try_new(a).unwrap();

  let batch = reader.next().unwrap().unwrap();

  match batch.column(1).as_any().downcast_ref::<arrow::array::Float64Array>() {
   Some(v) =>  v.value_slice(0, batch.num_rows()) .iter().fold(0.0, |acc, x| acc + x) as u32,
   None => 0
  }
}
