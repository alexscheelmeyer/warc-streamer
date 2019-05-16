const fs = require('fs');
const zlib = require('zlib');
const warc = require('.');

// const warcFile = `${__dirname}/../../../Downloads/0013wb-88.warc.gz`;
// const warcFile = `${__dirname}/../../../Downloads/rent/arto_20161107025000.megawarc.warc.gz`;
// const warcFile = `${__dirname}/../../../Downloads/CC-NEWS-20170731094200-00001.warc.gz`;
const warcFile = `${__dirname}/../../../Downloads/temp/CC-MAIN-20180920101233-20180920121606-00023.warc.gz`;

let numRecords = 0;
fs.createReadStream(warcFile)
  .pipe(zlib.createGunzip())
  .pipe(new warc.ReadTransform())
  .on('data', (record) => {
    numRecords++;
    if (numRecords % 100 === 0) {
      console.log(numRecords);
    }
    if (record.type === 'request') {
      // console.log('request', record);
    }
    else if (record.type === 'response') {
      console.log('response', record.header['WARC-Target-URI']);
      if (record.block.body) {
        // console.log(record.block.body.toString());
      }
    }
    else if (record.type === 'metadata') {
      console.log('metadata', record);
      // console.log(record.block.fields);
    }
    else {
      console.log(record.type);
    }
  })
  .on('error', (err) => {
    console.log('Error', err);
  });

