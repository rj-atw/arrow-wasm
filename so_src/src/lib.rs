//
// This file and its contents are supplied under the terms of the
// Common Development and Distribution License ("CDDL"), version 1.0.
// You may only use this file in accordance with the terms of version
// 1.0 of the CDDL.
//
// A full copy of the text of the CDDL should have accompanied this
// source.  A copy of the CDDL is also available via the Internet at
// https://opensource.org/licenses/CDDL-1.0
//


use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn readme() -> String { 
  return String::from("Calculates the variance")
}

#[wasm_bindgen]
pub fn reduce(a: &[u8]) -> u32 { 
  let mut reader = arrow::ipc::reader::StreamReader::try_new(a).unwrap();

  let batch = reader.next().unwrap().unwrap();

  match batch.column(4).as_any().downcast_ref::<arrow::array::Float64Array>().and_then(|v| arrow::compute::sum(v)) {
   Some(v) =>  v as u32, 
   None => 0
  }
}

#[wasm_bindgen]
pub fn map(a: &[u8]) -> *const f64 { 
  let mut reader = arrow::ipc::reader::StreamReader::try_new(a).unwrap();

  let batch = reader.next().unwrap().unwrap();
  
  let arr = arrow::compute::kernels::sort::sort(batch.column(4), None).unwrap(); 
  //let arr = batch.column(2);
  let arr_sorted = arrow::compute::limit(&arr, 500).unwrap();
  //let v = arr_sorted.as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
  let v = arr_sorted.as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();

  v.raw_values() as *const f64
}

#[wasm_bindgen]
pub fn filter(a: &[u8], f: &[u8]) -> *const f64 { 
  let mut reader = arrow::ipc::reader::StreamReader::try_new(a).unwrap();

  let batch = reader.next().unwrap().unwrap();
  
  let mut builder = arrow::array::PrimitiveBuilder::<arrow::datatypes::BooleanType>::new(f.len());

  //builder.append_slice( unsafe { mem::transmute::<&[u8], &[bool]>(f) });
  for &b in f {
    builder.append_value(if b > 0 {true} else {false}).unwrap();
  }

  let filter = builder.finish();


  let farr = arrow::compute::filter(batch.column(4).as_ref(), &filter).unwrap();
  let arr = arrow::compute::kernels::sort::sort(&farr, None).unwrap(); 
  let arr_sorted = arrow::compute::limit(&arr, 500).unwrap();
  let v = arr_sorted.as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();

  v.raw_values() as *const f64
}

fn limit_sorted_filter_propagate_error(
  a: &[u8], f: &[u8], idx: u32, limit: u32, out: *mut u32) 
-> Result<*const u32, arrow::error::ArrowError> { 
   let mut reader = arrow::ipc::reader::StreamReader::try_new(a)?;


  //TODO: handle None case i.e. an empty batch
  let batch = match reader.next()? {
    Some(x) => Ok(x),
    None => Err(arrow::error::ArrowError::ParseError(String::from("Arrow serial data contained no data")))
  }?;
  

  let arr = arrow::compute::kernels::sort::sort_to_indices(
    &batch.column(idx as usize),
    Some(arrow::compute::kernels::sort::SortOptions{ 
      descending: false, 
      nulls_first: true
    })
  )?;


  let mut output_index = 0;
  let mut builder = arrow::array::PrimitiveBuilder::<arrow::datatypes::UInt32Type>::new(f.len());
  let mut iter = arr. value_slice(0, arr.len()).iter();
  while output_index < limit {
    match iter.next() {
      Some(idx) => {
        if f[*idx as usize] > 0 {
          builder.append_value(*idx)?;
          output_index += 1;
        }
      }
      None => {
        break;
      }
    }
  }

  let v = builder.finish();

  unsafe {
      std::ptr::copy_nonoverlapping(v.raw_values(), out, limit as usize)
  }

  Ok(out)
}
 
/*
  Has 20 byte of error message by convention
*/
#[wasm_bindgen]
pub fn limit_sorted_filter(a: &[u8], f: &[u8], idx: u32, limit: u32, out: *mut u32, err: *mut u8) -> *const u32 { 
  let rtn = limit_sorted_filter_propagate_error(a,f,idx,limit,out);

  match rtn {
    Ok(_) => {
      out
    }
    Err(e) => {
      unsafe {
        let bytes_to_copy = std::cmp::max(e.to_string().as_bytes().len(), 200);
        std::ptr::copy_nonoverlapping(e.to_string().as_bytes().as_ptr(), err, bytes_to_copy);
      }
      0 as *const u32
    }
  }
}
