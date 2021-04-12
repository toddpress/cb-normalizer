import { createReadStream } from 'fs';
import csv from 'csv';
import glob from 'glob';
import { promisify } from 'util';

const globAsync = promisify(glob);

export const getFilesFromGlobPattern = async (pattern) => {
    const files = await globAsync(pattern);
    return files;
};

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

  console.log(output);
  return output;
};

(async () => {
  const records = await getFilesFromGlobPattern('./*.csv');
  console.log(`\n\nFILES: \n`, JSON.stringify(records, null, 4));
})();
