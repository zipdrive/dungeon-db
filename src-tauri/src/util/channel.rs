use std::io::BufWriter;
use std::marker::PhantomData;
use serde::{Serialize};
use serde_json::Serializer;
use tauri::ipc::{ Channel as TauriChannel };
use crate::util::error;

/// A wrapper for a raw Tauri channel.
pub struct Channel<'a, T: Serialize> {
    inner_channel: &'a TauriChannel,
    phantom: PhantomData<T>
}

impl<'a, T: Serialize> Channel<'a, T> {
    /// Creates a new channel wrapper.
    pub fn new(channel: &'a TauriChannel) -> Channel<'a, T> { 
        Channel::<T> {
            inner_channel: channel,
            phantom: PhantomData
        }
    }

    /// Sends a value through the channel.
    pub fn send(&self, value: T) -> Result<(), error::Error> {
        let writer = BufWriter::new(Vec::new());
        let mut serializer = Serializer::new(writer);
        match value.serialize(&mut serializer) {
            Err(_) => {
                return Err(error::Error::AdhocError("Unable to serialize value to JSON."));
            },
            _ => {}
        };
        let raw: Vec<u8> = match serializer.into_inner().into_inner() {
            Ok(r) => r,
            Err(_) => {
                return Err(error::Error::AdhocError("Unable to extract JSON buffer after serialization."));
            }
        };
        self.inner_channel.send(tauri::ipc::InvokeResponseBody::Raw(raw))?;
        return Ok(());
    }
}