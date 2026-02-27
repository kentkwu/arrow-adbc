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

import anyTest, { TestFn } from 'ava'
import { createSqliteDatabase, createTestTable, dumpReader } from './test_utils'
import { AdbcDatabase, AdbcConnection, AdbcStatement } from '../lib/index.js'
import { tableFromArrays } from 'apache-arrow'

interface TestContext {
  db: AdbcDatabase
  conn: AdbcConnection
  stmt: AdbcStatement
}

const test = anyTest as TestFn<TestContext>

test.before(async (t) => {
  const db = await createSqliteDatabase()
  const conn = await db.connect()
  const stmt = await conn.createStatement()

  await createTestTable(stmt, 'query_test')
  await stmt.setSqlQuery(`INSERT INTO query_test (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')`)
  await stmt.executeUpdate()

  t.context = { db, conn, stmt }
})

test.after.always(async (t) => {
  try {
    if (t.context.stmt) await t.context.stmt.close()
    if (t.context.conn) await t.context.conn.close()
    if (t.context.db) await t.context.db.close()
  } catch {
    // ignore
  }
})

test.serial('query: SELECT returns correct rows', async (t) => {
  const { stmt } = t.context

  await stmt.setSqlQuery('SELECT id, name FROM query_test ORDER BY id')
  const reader = await stmt.executeQuery()

  const rows: { id: unknown; name: unknown }[] = []
  for await (const batch of reader) {
    const idCol = batch.getChild('id')
    const nameCol = batch.getChild('name')
    for (let i = 0; i < batch.numRows; i++) {
      rows.push({ id: idCol?.get(i), name: nameCol?.get(i) })
    }
  }

  t.is(rows.length, 3)
  t.like(rows[0], { name: 'alice' })
  t.like(rows[1], { name: 'bob' })
  t.like(rows[2], { name: 'carol' })
})

test.serial('query: executeUpdate returns affected row count', async (t) => {
  const { conn } = t.context

  const stmt = await conn.createStatement()
  try {
    await stmt.setSqlQuery(`UPDATE query_test SET name = 'updated' WHERE id = 1`)
    const affected = await stmt.executeUpdate()
    t.is(typeof affected, 'number', 'executeUpdate should return a number')
    t.is(affected, 1, 'Should return 1 affected row')
  } finally {
    await stmt.close()
  }
})

test.serial('query: conn.query() returns correct rows', async (t) => {
  const { conn } = t.context

  // id=2 (bob) is never mutated by other tests in this file
  const rows = await dumpReader(await conn.query('SELECT id, name FROM query_test WHERE id = 2'))
  t.is(rows.length, 1)
  t.is(rows[0].name, 'bob')
})

test.serial('query: conn.execute() returns affected row count', async (t) => {
  const { conn } = t.context

  const affected = await conn.execute(`UPDATE query_test SET name = 'via_execute' WHERE id = 3`)
  t.is(affected, 1)
})

test.serial('query: conn.execute() with bound params inserts a row', async (t) => {
  const { conn } = t.context

  const params = tableFromArrays({ id: [99], name: ['bound_insert'] })
  const affected = await conn.execute('INSERT INTO query_test (id, name) VALUES (?, ?)', params)
  t.is(affected, 1)

  const rows = await dumpReader(await conn.query('SELECT name FROM query_test WHERE id = 99'))
  t.is(rows.length, 1)
  t.is(rows[0].name, 'bound_insert')
})

test.serial('query: empty result set', async (t) => {
  const { conn } = t.context

  // Use a fresh statement — ADBC statements should not be reused after executeQuery
  const stmt = await conn.createStatement()
  try {
    await stmt.setSqlQuery('SELECT * FROM query_test WHERE id = 9999')
    const reader = await stmt.executeQuery()

    let rowCount = 0
    for await (const batch of reader) {
      rowCount += batch.numRows
    }

    t.is(rowCount, 0, 'Should return 0 rows for no-match query')
  } finally {
    await stmt.close()
  }
})
