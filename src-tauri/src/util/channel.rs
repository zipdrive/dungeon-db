use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use serde::{Serialize};
use serde_json::Serializer;
use tauri::{AppHandle,Emitter};
use tauri::ipc::{ Channel as TauriChannel };
use crate::util::error;

pub enum Sender<'a, T: Serialize + Clone> {
    Channel(TauriChannel<T>),
    Event(&'a AppHandle, &'static str)
}

impl<'a, T: Serialize + Clone> Sender<'a, T> {
    pub fn send(&self, payload: T) -> Result<(), error::Error> {
        match self {
            Self::Channel(channel) => {
                channel.send(payload)?;
            }
            Self::Event(app, event_name) => {
                app.emit(event_name, payload)?;
            }
        }
        return Ok(());
    }
}