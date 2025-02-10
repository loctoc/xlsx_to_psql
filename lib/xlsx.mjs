import XLSX from 'xlsx';
import moment from 'moment-timezone';

function sanitizeColumnName(name) {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .replace(/_+/g, '_');
}

function transformValue(value, config, timezone) {
  if (value === '' || value === '-' || value === undefined || value === null) return null;

  try {
    switch (config.fieldType?.toLowerCase()) {
      case 'timestamp':
        // Handle Excel date numbers
        if (typeof value === 'number') {
          const excelDate = XLSX.SSF.parse_date_code(value);
          const dateStr = `${excelDate.y}-${String(excelDate.m).padStart(2, '0')}-${String(excelDate.d).padStart(2, '0')} ${String(excelDate.H).padStart(2, '0')}:${String(excelDate.M).padStart(2, '0')}`;
          const m = moment.tz(dateStr, 'YYYY-MM-DD HH:mm', timezone);
          if (!m.isValid()) {
            console.warn(`⚠️ Invalid date format: ${value}`);
            return null;
          }
          return m.toISOString();
        }
        // Handle string dates
        const m = moment.tz(value, 'YYYY-MM-DD HH:mm', timezone);
        if (!m.isValid()) {
          console.warn(`⚠️ Invalid date format: ${value}`);
          return null;
        }
        return m.toISOString();

      case 'number':
        const num = Number(value);
        return isNaN(num) ? null : num;

      case 'string':
      default:
        return String(value).replace(/\s+/g, ' ').trim();
    }
  } catch (error) {
    console.warn(`⚠️ Error transforming value: ${value}`, error.message);
    return null;
  }
}

export async function parseAndTransformXLSX(filePath, tableConfig, timezone, onProgress, sheetName = null) {
  return new Promise((resolve, reject) => {
    try {
      const startTime = Date.now();
      const columns = [];
      const transformedData = [];
      let headerIndexMap = new Map();
      let processedRows = 0;
      let skippedRows = 0;
      let emptyRows = 0;

      console.log(`\n📊 [${new Date().toISOString()}] Reading XLSX file...`);
      
      // Read the workbook
      const workbook = XLSX.readFile(filePath, {
        type: 'file',
        cellDates: true,
        cellNF: false,
        cellText: false
      });

      // List available sheets
      console.log('\n📑 Available sheets:', workbook.SheetNames);

      // Get sheet - uses first sheet if sheetName is null
      const targetSheet = sheetName || workbook.SheetNames[0];
      if (!workbook.SheetNames.includes(targetSheet)) {
        throw new Error(`Sheet "${targetSheet}" not found. Available sheets: ${workbook.SheetNames.join(', ')}`);
      }

      console.log(`📋 [${new Date().toISOString()}] Using sheet: ${targetSheet}`);
      const worksheet = workbook.Sheets[targetSheet];

      // Convert to array of arrays
      const data = XLSX.utils.sheet_to_json(worksheet, {
        header: 1,
        raw: true,
        defval: null
      });

      if (data.length < 2) {
        throw new Error('File is empty or has no data rows');
      }

      // Process headers (first row)
      const headers = data[0];
      headers.forEach((header, index) => {
        if (header) {
          const cleanHeader = String(header).replace(/\s+/g, ' ').trim();
          headerIndexMap.set(cleanHeader, index);
        }
      });

      // Map columns based on config
      tableConfig.forEach(config => {
        if (headerIndexMap.has(config.header)) {
          columns.push(config.sqlColumn || sanitizeColumnName(config.header));
        } else {
          console.log(`⚠️ [${new Date().toISOString()}] Skipping missing column: ${config.header}`);
        }
      });

      console.log(`📋 [${new Date().toISOString()}] Columns found: ${columns.length}`);

      // Process data rows
      const dataRows = data.length - 1; // Exclude header row
      console.log(`📊 [${new Date().toISOString()}] Processing ${dataRows} rows...`);

      for (let rowIndex = 1; rowIndex < data.length; rowIndex++) {
        const row = data[rowIndex];
        
        // Check if row is empty
        if (!row || row.every(val => val === null || val === undefined || String(val).trim() === '')) {
          emptyRows++;
          continue;
        }

        // Transform row values
        const transformedRow = columns.map(colName => {
          const config = tableConfig.find(c =>
            (c.sqlColumn || sanitizeColumnName(c.header)) === colName
          );
          if (!config) return null;

          const index = headerIndexMap.get(config.header);
          const value = row[index];
          return transformValue(value, config, timezone);
        });

        // Skip row if all values are null
        if (transformedRow.every(val => val === null)) {
          skippedRows++;
          continue;
        }

        transformedData.push(transformedRow);
        processedRows++;

        // Update progress periodically
        if (rowIndex % 1000 === 0) {
          const progress = (rowIndex / dataRows) * 100;
          onProgress(progress);
        }
      }

      const duration = ((Date.now() - startTime) / 1000).toFixed(2);
      console.log(`
📊 XLSX Processing Summary:
   Sheet name: ${targetSheet}
   Total rows in file: ${dataRows}
   Empty rows skipped: ${emptyRows}
   Invalid rows skipped: ${skippedRows}
   Valid rows transformed: ${transformedData.length}
   Processing time: ${duration}s
`);

      if (transformedData.length === 0) {
        throw new Error('No valid data found in the XLSX file');
      }

      // Log a sample row
      console.log('\n📝 Sample transformed row:');
      console.log(columns.reduce((obj, col, i) => {
        obj[col] = transformedData[0][i];
        return obj;
      }, {}));

      resolve({ columns, transformedData });

    } catch (error) {
      console.error(`⚠️ [${new Date().toISOString()}] Error processing XLSX: ${error.message}`);
      reject(new Error(`Failed to process XLSX: ${error.message}`));
    }
  });
}