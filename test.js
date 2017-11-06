const fs = require('fs');
const zlib = require('zlib');
const warc = require('.');

// const warcFile = `${__dirname}/../../../Downloads/0013wb-88.warc.gz`;
const warcFile = `${__dirname}/../../../Downloads/rent/arto_20161107025000.megawarc.warc.gz`;
// const warcFile = `${__dirname}/../../../Downloads/CC-NEWS-20170731094200-00001.warc.gz`;
// const warcFile = `${__dirname}/../../../Downloads/CC-MAIN-20171016214209-20171016234209-00001.warc.gz`;

let numRecords = 0;
fs.createReadStream(warcFile)
  .pipe(zlib.createGunzip())
  .pipe(new warc.ReadTransform())
  .on('data', (record) => {
    numRecords++;
    if (numRecords % 100 === 0) {
      console.log(numRecords);
    }
    // if (record.type === 'response') {
    //   console.log(record.header['WARC-Target-URI']);
    // }
    if (record.type === 'metadata') {
      console.log(record.block.fields);
    }
  })
  .on('error', (err) => {
    console.log('Error', err);
  });

