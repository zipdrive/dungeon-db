use std::sync::Mutex;

static CUR_PROCESSID: Mutex<i64> = Mutex::new(0);

/// Gets a unique processid.
pub fn get_processid() -> i64 {
    let mut cur_processid = CUR_PROCESSID.lock().unwrap();
    *cur_processid += 1;
    (*cur_processid).clone()
}