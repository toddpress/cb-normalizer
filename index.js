import { createReadStream } from 'fs';
import csv from 'csv';

// Data Maps
const UNIFIED_KEYS_BY_COLUMN = {
  'created at': 'Date(UTC)',
  'price/fee/total unit': 'Fee Coin',
  product: 'Market',
  size: 'Amount',
  price: 'Price',
  total: 'Total',
  side: 'Type',
  fee: 'Fee',
};

const VALUE_TRANSFORMERS_BY_ORIG_COLUMN = {
  'created at': (val) => val.replace(/(T|Z)+/g, ' ').trim(),
  product: (val) => val.replace(/\-/g, ''),
  total: (val) => val.replace(/\-/g, ''),
  side: (val) => val.toUpperCase(),
};

const getColumnFromMap = (map) => (column) => map[column];

const getUnifiedColumnName = (column) =>
  getColumnFromMap(UNIFIED_KEYS_BY_COLUMN)(column);

const getColumnTransformFromMap = (map) => (column) =>
  typeof map[column] === 'function' ? map[column] : (val) => val;

const getTransformerForColumn = (column) =>
  getColumnTransformFromMap(VALUE_TRANSFORMERS_BY_ORIG_COLUMN)(column);

const transformCoinbaseEntry = ([k, v], i) => {
  const unifiedColumn = getUnifiedColumnName(k);
  debugger;
  const transformedValue = getTransformerForColumn(k)(v);
  return [unifiedColumn, transformedValue];
};

const transformResults = (obj, mappingFn) =>
  Object.fromEntries(Object.entries(obj).map(mappingFn).filter(([k, v]) => k !== undefined));

const processFile = async (url) => {
  let output = [];
  const parser = createReadStream(url).pipe(
    csv.parse({
      columns: true,
      skip_empty_lines: true,
      parallel: true,
    })
  );

  for await (const row of parser) {
    const transformed = transformResults(row, transformCoinbaseEntry);
    output.push(transformed);
  }

  return output;
};

// Declare the work.
const processCoinbaseRecords = async (url) => await processFile(url);
// Do the work.
(async () => {
  const records = await processCoinbaseRecords('./CB.3.31.2021.csv');
  console.log(`\n\nTRANSFORMED RESULTS: \n`, JSON.stringify(records, null, 4));
})();
