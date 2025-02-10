import pg from 'pg';
import format from 'pg-format';
const { Pool } = pg;

let pool = null;

const startTime = Date.now();

// Helper function to format elapsed time
function formatElapsed(startTime) {
  const elapsed = Date.now() - startTime;
  if (elapsed < 1000) return `${elapsed}ms`;
  return `${(elapsed/1000).toFixed(2)}s`;
}

// Helper function to log queries
function logQuery(query, params) {
  // Skip logging INSERT queries
  if (query.trim().toUpperCase().startsWith('INSERT')) {
    return;
  }
  
  const timestamp = new Date().toISOString();
  const elapsed = formatElapsed(startTime);
  console.log(`\n🔍 [${timestamp}] Executing Query (${elapsed}):`);
  console.log(query);
  if (params && params.length > 0) {
    console.log('Parameters:', params);
  }
}

export function initializeDB(connectionString) {
  pool = new Pool({ connectionString });
  return pool;
}

async function tableExists(client, tableName) {
  const [schema, table] = tableName.split('.');
  const query = `
    SELECT EXISTS (
      SELECT FROM information_schema.tables 
      WHERE table_schema = $1 
      AND table_name = $2
    )
  `;
  logQuery(query, [schema, table]);
  const result = await client.query(query, [schema, table]);
  return result.rows[0].exists;
}

function getColumnDefinition(config) {
  let definition = `"${config.sqlColumn}" `;
  
  switch(config.fieldType?.toLowerCase()) {
    case 'timestamp':
      definition += 'TIMESTAMP';
      break;
    case 'number':
      definition += 'NUMERIC';
      break;
    default:
      definition += 'TEXT';
  }

  if (config.primary) {
    definition += ' PRIMARY KEY';
  }
  if (config.notNull) {
    definition += ' NOT NULL';
  }

  return definition;
}

export async function createTempTable(tableName, columns, tableConfig, timestamp) {
  if (!pool) {
    throw new Error('Database not initialized');
  }

  const client = await pool.connect();
  const [schema, table] = tableName.split('.');
  const tmpTableName = `${schema}.${table}_tmp`;
  
  try {
    await client.query('BEGIN');
    logQuery('BEGIN');

    // Drop temp table if exists
    const dropQuery = `DROP TABLE IF EXISTS ${tmpTableName}`;
    logQuery(dropQuery);
    await client.query(dropQuery);

    // Create column definitions
    const columnDefinitions = columns.map(col => {
      const config = tableConfig.find(c => 
        (c.sqlColumn || sanitizeColumnName(c.header)) === col
      );
      return getColumnDefinition(config);
    }).join(',\n    ');

    // Create temp table
    const createQuery = `
      CREATE TABLE ${tmpTableName} (
        ${columnDefinitions}
      )
    `;
    logQuery(createQuery);
    await client.query(createQuery);

    // Create indexes with timestamp suffix
    for (const config of tableConfig) {
      if (config.needIndex) {
        const indexName = `idx_${table}_${config.sqlColumn}_${timestamp}`;
        const createIndexQuery = `
          CREATE INDEX ${indexName} ON ${tmpTableName} ("${config.sqlColumn}")
        `;
        logQuery(createIndexQuery);
        await client.query(createIndexQuery);
      }
    }

    await client.query('COMMIT');
    logQuery('COMMIT');
    
    return { tmpTableName };
  } catch (error) {
    await client.query('ROLLBACK');
    logQuery('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

export async function insertBatch(tableName, columns, batch) {
  if (!pool) {
    throw new Error('Database not initialized');
  }

  try {
    // Format the insert query using pg-format
    const query = format(
      'INSERT INTO %I.%I (%s) VALUES %L',
      tableName.split('.')[0].toLowerCase(),
      tableName.split('.')[1].toLowerCase(),
      columns.map(c => `"${c}"`).join(', '),
      batch
    );

    // For debugging
    // image.pngconsole.log(`📊 Inserting batch of ${batch.length} rows`);
    
    await pool.query(query);
    return batch.length;
  } catch (error) {
    console.error('\nError details:', {
      batchSize: batch.length,
      columnsCount: columns.length,
      firstRow: batch[0],
      error: error.message
    });
    throw error;
  }
}

export async function swapTables(tmpTableName, originalTable, shouldTruncate) {
  if (!pool) {
    throw new Error('Database not initialized');
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    logQuery('BEGIN');

    const [schema, table] = originalTable.split('.');

    if (shouldTruncate) {
      // Drop original and rename temp
      const dropQuery = `DROP TABLE IF EXISTS ${originalTable}`;
      logQuery(dropQuery);
      await client.query(dropQuery);

      const renameQuery = `ALTER TABLE ${tmpTableName} RENAME TO ${table}`;
      logQuery(renameQuery);
      await client.query(renameQuery);
    } else {
      // Create original table if it doesn't exist
      const exists = await tableExists(client, originalTable);
      if (!exists) {
        const createLikeQuery = `
          CREATE TABLE ${originalTable} (LIKE ${tmpTableName} INCLUDING ALL)
        `;
        logQuery(createLikeQuery);
        await client.query(createLikeQuery);
      }

      // Insert data from temp to original
      const insertQuery = `
        INSERT INTO ${originalTable}
        SELECT * FROM ${tmpTableName}
      `;
      logQuery(insertQuery);
      await client.query(insertQuery);

      // Drop temp table
      const dropTempQuery = `DROP TABLE ${tmpTableName}`;
      logQuery(dropTempQuery);
      await client.query(dropTempQuery);
    }

    await client.query('COMMIT');
    logQuery('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    logQuery('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

export async function closeDB() {
  if (pool) {
    await pool.end();
    pool = null;
  }
} 