import XLSX from 'xlsx';
import fs from 'fs';
export function getTableConfigOverrides(tableConfigFile) {
    if (tableConfigFile && fs.existsSync(tableConfigFile)) {
        const tableConfig = JSON.parse(fs.readFileSync(tableConfigFile, 'utf-8'));
        return tableConfig
    }
    return {}
}
export function getTableConfigForAWorkSheet(worksheet, tableConfigFile) {
    const tableConfigOverrides = getTableConfigOverrides(tableConfigFile);
    const range = XLSX.utils.decode_range(worksheet['!ref'] || 'A1');
    const headersUsed = {}

    const tableConfig = Array(range.e.c).fill(1).map((_, idx) => {
        const cellAddress = XLSX.utils.encode_cell({ r: 0, c: idx }); // headers
        const cell = worksheet[cellAddress];
        let colVal = cell.v;
        if (headersUsed[colVal]) {
            headersUsed[colVal] = (headersUsed[colVal] || 1) + 1
            colVal = `${colVal} ${headersUsed[colVal]}`
        }
        const colOverrides = tableConfigOverrides?.[colVal] ?? {}
        return {
            "header": colVal,
            "sqlColumn": colVal,
            "fieldType": "string",
            "primary": false,
            "notNull": false,
            "skip": false,
            "needIndex": false,
            "isHyperlink": true,
            ...colOverrides
        }
    })
    return tableConfig
}