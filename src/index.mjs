#!/usr/bin/env node

import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import cliProgress from 'cli-progress';
import dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';
import { parseAndTransformCSV } from '../lib/csv.mjs';
import { parseAndTransformXLSX } from '../lib/xlsx.mjs';
import { initializeDB, createTempTable, insertBatch, swapTables, closeDB } from '../lib/db.mjs';
import { sendSlackNotification } from '../lib/notifications.mjs';
import moment from 'moment';

// Load environment variables first
dotenv.config();

if (!process.env.DATABASE_URL) {
  console.error('❌ DATABASE_URL environment variable is not set');
  process.exit(1);
}

// Create progress bars
const multibar = new cliProgress.MultiBar({
  clearOnComplete: false,
  hideCursor: true,
  format: '{bar} {percentage}% | {value}/{total} {task}'
}, cliProgress.Presets.shades_classic);

async function main() {
  // 1. Parse CLI arguments
  const argv = yargs(hideBin(process.argv))
    .option('input-file', {
      describe: 'Path to input Excel or CSV file',
      type: 'string',
      demandOption: true
    })
    .option('table', {
      describe: 'Target PostgreSQL table (format: schema.table)',
      type: 'string',
      demandOption: true
    })
    .option('table-config', {
      describe: 'JSON file containing table configuration',
      type: 'string',
      demandOption: true
    })
    .option('timezone', {
      describe: 'Timezone for date parsing (e.g., Asia/Kolkata)',
      type: 'string',
      demandOption: true
    })
    .option('batch-size', {
      describe: 'Number of records per batch insert',
      type: 'number',
      default: 5000
    })
    .option('truncate', {
      describe: 'Truncate table before import',
      type: 'boolean',
      default: false
    })
    .option('slack-notify-url', {
      describe: 'Slack webhook URL for notifications',
      type: 'string'
    })
    .option('sheet-name', {
      describe: 'Excel sheet name (defaults to first sheet)',
      type: 'string'
    })
    .check((argv) => {
      // Validate file exists
      if (!fs.existsSync(argv.inputFile)) {
        throw new Error(`Input file not found: ${argv.inputFile}`);
      }
      // Validate table config exists
      if (!fs.existsSync(argv.tableConfig)) {
        throw new Error(`Table config file not found: ${argv.tableConfig}`);
      }
      // Validate file extension
      const ext = path.extname(argv.inputFile).toLowerCase();
      if (!['.csv', '.xlsx', '.xls'].includes(ext)) {
        throw new Error(`Unsupported file type: ${ext}. Only .csv, .xlsx, and .xls files are supported`);
      }
      return true;
    })
    .argv;

  try {
    // Initialize database connection first
    console.log(`\n🔌 [${new Date().toISOString()}] Initializing database connection...`);
    initializeDB(process.env.DATABASE_URL);

    // 2. Read and validate table configuration
    const tableConfig = JSON.parse(fs.readFileSync(argv.tableConfig, 'utf-8'));
    console.log(`📋 [${new Date().toISOString()}] Loaded table configuration`);
    console.log(`🕒 [${new Date().toISOString()}] Using timezone: ${argv.timezone}`);
    console.log(`📦 [${new Date().toISOString()}] Batch size: ${argv.batchSize}`);

    // 3. Parse and transform data based on file type
    const parseBar = multibar.create(100, 0, { task: 'Parsing file' });
    const fileExt = path.extname(argv.inputFile).toLowerCase();
    
    const { columns, transformedData, emptyRows, skippedRows } = await (fileExt === '.csv' 
      ? parseAndTransformCSV(
          argv.inputFile,
          tableConfig,
          argv.timezone,
          (progress) => parseBar.update(progress)
        )
      : parseAndTransformXLSX(
          argv.inputFile,
          tableConfig,
          argv.timezone,
          (progress) => parseBar.update(progress),
          argv.sheetName
        )
    );
    parseBar.stop();

    // 4. Create temporary table with timestamp-suffixed indexes
    const timestamp = moment().format('YYYYMMDDHHMMSS');
    const { tmpTableName } = await createTempTable(argv.table, columns, tableConfig, timestamp);

    // 5. Insert data in batches
    console.log('\nInserting data...');
    const insertBar = multibar.create(transformedData.length, 0, { task: 'Inserting rows' });
    let insertedRows = 0;
    const startTime = Date.now();

    for (let i = 0; i < transformedData.length; i += argv.batchSize) {
      const batch = transformedData.slice(i, i + argv.batchSize);
      await insertBatch(tmpTableName, columns, batch);
      insertedRows += batch.length;
      insertBar.update(insertedRows);
    }
    insertBar.stop();

    // 6 & 7. Handle table swap based on truncate option
    if (argv.truncate) {
      console.log('\nSwapping tables...');
      await swapTables(tmpTableName, argv.table, true);
    } else {
      console.log('\nMerging data...');
      await swapTables(tmpTableName, argv.table, false);
    }

    multibar.stop();

    const summaryData = {
      inputFile: path.basename(argv.inputFile),
      tableName: argv.table,
      totalRows: transformedData.length + emptyRows + skippedRows,
      validRows: insertedRows,
      emptyRows,
      skippedRows,
      duration: ((Date.now() - startTime) / 1000).toFixed(2),
      sheetName: argv.sheetName
    };

    const successMessage = `✅ Successfully imported ${insertedRows} rows into ${argv.table}`;
    console.log(`\n${successMessage}`);
    await sendSlackNotification(argv.slackNotifyUrl, successMessage, summaryData);

  } catch (error) {
    multibar.stop();
    const errorMessage = `❌ Error importing data: ${error.message}`;
    console.error(`\n${errorMessage}`);
    await sendSlackNotification(argv.slackNotifyUrl, errorMessage);
    process.exit(1);
  } finally {
    await closeDB();
  }
}

main(); 