const fs = require('fs');
const zlib = require('zlib');
const warc = require('.');

const warcFile = `${__dirname}/../../../Downloads/temp/CC-MAIN-20180918130631-20180918150631-00000.warc.gz`;

let numRecords = 0;
const startTime = process.hrtime()[0];
fs.createReadStream(warcFile)
  .pipe(zlib.createGunzip())
  .pipe(new warc.ReadTransform())
  .on('data', () => {
    numRecords++;
  })
  .on('error', (err) => {
    console.error('Error', err);
  })
  .on('end', () => {
    const seconds = (process.hrtime()[0] - startTime);
    console.log(`Done ${numRecords} records in ${seconds} seconds`);
  });

