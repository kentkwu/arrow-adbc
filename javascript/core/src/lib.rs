// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use adbc_core::{
  options::{
    AdbcVersion, InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement,
    OptionValue,
  },
  Connection, Database, Driver, Optionable, Statement, LOAD_FLAG_DEFAULT,
};
use adbc_driver_manager::{ManagedConnection, ManagedDatabase, ManagedDriver, ManagedStatement};
use arrow_array::RecordBatchReader;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
  #[error("ADBC Error: {0}")]
  Adbc(#[from] adbc_core::error::Error),
  #[error("Arrow Error: {0}")]
  Arrow(#[from] arrow_schema::ArrowError),
  #[error("Other Error: {0}")]
  Other(String),
}

pub type Result<T> = std::result::Result<T, ClientError>;

pub struct ConnectOptions {
  pub driver: String,
  pub entrypoint: Option<String>,
  pub search_paths: Option<Vec<String>>,
  pub load_flags: Option<u32>,
  pub database_options: Option<HashMap<String, String>>,
}

pub struct GetObjectsOptions {
  pub depth: i32,
  pub catalog: Option<String>,
  pub db_schema: Option<String>,
  pub table_name: Option<String>,
  pub table_type: Option<Vec<String>>,
  pub column_name: Option<String>,
}

pub struct GetTableSchemaOptions {
  pub catalog: Option<String>,
  pub db_schema: Option<String>,
  pub table_name: String,
}

pub struct AdbcDatabaseCore {
  inner: ManagedDatabase,
}

impl AdbcDatabaseCore {
  pub fn new(opts: ConnectOptions) -> Result<Self> {
    let version = AdbcVersion::V110;
    let load_flags = opts.load_flags.unwrap_or(LOAD_FLAG_DEFAULT);
    let entrypoint = opts.entrypoint.as_ref().map(|s| s.as_bytes().to_vec());

    let search_paths: Option<Vec<PathBuf>> = opts
      .search_paths
      .map(|paths| paths.into_iter().map(PathBuf::from).collect());

    let mut driver = ManagedDriver::load_from_name(
      &opts.driver,
      entrypoint.as_deref(),
      version,
      load_flags,
      search_paths,
    )?;

    let database = if let Some(db_map) = opts.database_options {
      driver.new_database_with_opts(map_database_options(db_map))?
    } else {
      driver.new_database()?
    };

    Ok(Self { inner: database })
  }

  pub fn connect(&self, options: Option<HashMap<String, String>>) -> Result<AdbcConnectionCore> {
    let conn = if let Some(opts) = options {
      self
        .inner
        .new_connection_with_opts(map_connection_options(opts))?
    } else {
      self.inner.new_connection()?
    };
    Ok(AdbcConnectionCore {
      inner: Mutex::new(conn),
    })
  }
}

pub struct AdbcConnectionCore {
  inner: Mutex<ManagedConnection>,
}

impl AdbcConnectionCore {
  pub fn new_statement(&self) -> Result<AdbcStatementCore> {
    let mut conn = self
      .inner
      .lock()
      .map_err(|e| ClientError::Other(e.to_string()))?;
    let stmt = conn.new_statement()?;
    Ok(AdbcStatementCore { inner: stmt })
  }

  pub fn set_option(&self, key: &str, value: &str) -> Result<()> {
    let mut conn = self
      .inner
      .lock()
      .map_err(|e| ClientError::Other(e.to_string()))?;
    conn.set_option(
      OptionConnection::Other(key.to_string()),
      OptionValue::String(value.to_string()),
    )?;
    Ok(())
  }

  pub fn get_objects(&self, opts: GetObjectsOptions) -> Result<AdbcConnectionResultIteratorCore> {
    let conn = self
      .inner
      .lock()
      .map_err(|e| ClientError::Other(e.to_string()))?;
    let depth = match opts.depth {
      0 => ObjectDepth::All,
      1 => ObjectDepth::Catalogs,
      2 => ObjectDepth::Schemas,
      3 => ObjectDepth::Tables,
      _ => ObjectDepth::All,
    };

    let table_types_str: Option<Vec<&str>> = opts
      .table_type
      .as_ref()
      .map(|v| v.iter().map(|s| s.as_str()).collect());

    let reader = conn.get_objects(
      depth,
      opts.catalog.as_deref(),
      opts.db_schema.as_deref(),
      opts.table_name.as_deref(),
      table_types_str,
      opts.column_name.as_deref(),
    )?;

    // SAFETY: The reader's lifetime is parameterized by the ManagedConnection
    // reference obtained from the MutexGuard. We clone the ManagedConnection
    // (which is Arc-based) into the returned struct, ensuring it outlives the
    // MutexGuard that is dropped at the end of this function. The reader
    // therefore remains valid for the lifetime of the returned iterator.
    let reader: Box<dyn RecordBatchReader + Send> =
      unsafe { std::mem::transmute(Box::new(reader) as Box<dyn RecordBatchReader + Send>) };

    Ok(AdbcConnectionResultIteratorCore {
      reader,
      _connection: conn.clone(),
      schema: None,
    })
  }

  pub fn get_table_schema(&self, opts: GetTableSchemaOptions) -> Result<Vec<u8>> {
    let conn = self
      .inner
      .lock()
      .map_err(|e| ClientError::Other(e.to_string()))?;
    let schema = conn.get_table_schema(
      opts.catalog.as_deref(),
      opts.db_schema.as_deref(),
      &opts.table_name,
    )?;

    let mut output = Vec::new();
    let mut writer = StreamWriter::try_new(&mut output, &schema)?;
    writer.finish()?;
    Ok(output)
  }

  pub fn get_table_types(&self) -> Result<AdbcConnectionResultIteratorCore> {
    let conn = self
      .inner
      .lock()
      .map_err(|e| ClientError::Other(e.to_string()))?;
    let reader = conn.get_table_types()?;

    // SAFETY: See get_objects for rationale. The cloned ManagedConnection in
    // the returned struct ensures it outlives the MutexGuard dropped here.
    let reader: Box<dyn RecordBatchReader + Send> =
      unsafe { std::mem::transmute(Box::new(reader) as Box<dyn RecordBatchReader + Send>) };

    Ok(AdbcConnectionResultIteratorCore {
      reader,
      _connection: conn.clone(),
      schema: None,
    })
  }

  pub fn get_info(
    &self,
    info_codes: Option<Vec<u32>>,
  ) -> Result<AdbcConnectionResultIteratorCore> {
    let conn = self
      .inner
      .lock()
      .map_err(|e| ClientError::Other(e.to_string()))?;
    let codes: Option<HashSet<InfoCode>> = info_codes.map(|v| {
      v.into_iter()
        .filter_map(|code| InfoCode::try_from(code).ok())
        .collect::<HashSet<InfoCode>>()
    });
    let reader = conn.get_info(codes)?;

    // SAFETY: See get_objects for rationale. The cloned ManagedConnection in
    // the returned struct ensures it outlives the MutexGuard dropped here.
    let reader: Box<dyn RecordBatchReader + Send> =
      unsafe { std::mem::transmute(Box::new(reader) as Box<dyn RecordBatchReader + Send>) };

    Ok(AdbcConnectionResultIteratorCore {
      reader,
      _connection: conn.clone(),
      schema: None,
    })
  }

  pub fn commit(&self) -> Result<()> {
    let mut conn = self
      .inner
      .lock()
      .map_err(|e| ClientError::Other(e.to_string()))?;
    conn.commit()?;
    Ok(())
  }

  pub fn rollback(&self) -> Result<()> {
    let mut conn = self
      .inner
      .lock()
      .map_err(|e| ClientError::Other(e.to_string()))?;
    conn.rollback()?;
    Ok(())
  }
}

pub struct AdbcStatementCore {
  inner: ManagedStatement,
}

impl AdbcStatementCore {
  pub fn set_sql_query(&mut self, query: &str) -> Result<()> {
    self.inner.set_sql_query(query)?;
    Ok(())
  }

  pub fn set_option(&mut self, key: &str, value: &str) -> Result<()> {
    self.inner.set_option(
      OptionStatement::Other(key.to_string()),
      OptionValue::String(value.to_string()),
    )?;
    Ok(())
  }

  pub fn execute_query(&mut self) -> Result<AdbcStatementIteratorCore> {
    let reader = self.inner.execute()?;

    // SAFETY: The reader's lifetime is parameterized by the ManagedStatement.
    // We clone it (Arc-based) into the returned struct, ensuring it outlives
    // the borrow and remains valid for the lifetime of the returned iterator.
    let reader: Box<dyn RecordBatchReader + Send> =
      unsafe { std::mem::transmute(Box::new(reader) as Box<dyn RecordBatchReader + Send>) };

    Ok(AdbcStatementIteratorCore {
      reader,
      _statement: self.inner.clone(),
      schema: None,
    })
  }

  pub fn execute_update(&mut self) -> Result<i64> {
    let rows = self.inner.execute_update()?;
    Ok(rows.unwrap_or(-1))
  }

  pub fn bind(&mut self, c_data: Vec<u8>) -> Result<()> {
    let mut reader = StreamReader::try_new(std::io::Cursor::new(c_data), None)
      .map_err(ClientError::Arrow)?;
    let batch = match reader.next() {
      Some(Ok(b)) => b,
      Some(Err(e)) => return Err(ClientError::Arrow(e)),
      None => {
        return Err(ClientError::Other(
          "bind() received an empty record batch stream".to_string(),
        ))
      }
    };
    if reader.next().is_some() {
      return Err(ClientError::Other(
        "bind() received multiple record batches; concatenate into one batch first".to_string(),
      ));
    }
    self.inner.bind(batch)?;
    Ok(())
  }
}

pub struct AdbcStatementIteratorCore {
  reader: Box<dyn RecordBatchReader + Send>,
  _statement: ManagedStatement,
  schema: Option<Arc<arrow_schema::Schema>>,
}

impl AdbcStatementIteratorCore {
  pub fn next(&mut self) -> Result<Option<Vec<u8>>> {
    read_next_batch(&mut self.reader, &mut self.schema)
  }
}

pub struct AdbcConnectionResultIteratorCore {
  reader: Box<dyn RecordBatchReader + Send>,
  _connection: ManagedConnection,
  schema: Option<Arc<arrow_schema::Schema>>,
}

impl AdbcConnectionResultIteratorCore {
  pub fn next(&mut self) -> Result<Option<Vec<u8>>> {
    read_next_batch(&mut self.reader, &mut self.schema)
  }
}

fn read_next_batch(
  reader: &mut Box<dyn RecordBatchReader + Send>,
  schema: &mut Option<Arc<arrow_schema::Schema>>,
) -> Result<Option<Vec<u8>>> {
  let schema = schema.get_or_insert_with(|| reader.schema());
  match reader.next() {
    Some(Ok(batch)) => {
      let mut output = Vec::new();
      let mut writer = StreamWriter::try_new(&mut output, schema)?;
      writer.write(&batch)?;
      writer.finish()?;
      Ok(Some(output))
    }
    Some(Err(e)) => Err(ClientError::Arrow(e)),
    None => Ok(None),
  }
}

fn map_database_options(
  opts: HashMap<String, String>,
) -> impl Iterator<Item = (OptionDatabase, OptionValue)> {
  opts.into_iter().map(|(k, v)| {
    let key = match k.as_str() {
      "uri" => OptionDatabase::Uri,
      "user" => OptionDatabase::Username,
      "password" => OptionDatabase::Password,
      other => OptionDatabase::Other(other.to_string()),
    };
    (key, OptionValue::String(v))
  })
}

fn map_connection_options(
  opts: HashMap<String, String>,
) -> impl Iterator<Item = (OptionConnection, OptionValue)> {
  opts
    .into_iter()
    .map(|(k, v)| (OptionConnection::Other(k), OptionValue::String(v)))
}
