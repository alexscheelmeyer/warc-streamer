const fs = require('fs');
const zlib = require('zlib');
const warc = require('.');

const warcFile = `${__dirname}/../../../Downloads/temp/CC-MAIN-20180920101233-20180920121606-00023.warc.gz`;

/*

{"WARC-Type":"response",
"WARC-Date":"2018-09-20T10:16:36Z",
"WARC-Record-ID":"<urn:uuid:de247d6e-3328-4870-a6c5-51ea094fd4d4>",
"Content-Length":1764,
"Content-Type":"application/http; msgtype=response",
"WARC-Warcinfo-ID":"<urn:uuid:7ac1ef6d-4974-495b-954f-13cf379a2c97>",
"WARC-Concurrent-To":"<urn:uuid:2d36dc8b-9f03-43e1-9fd0-df8ba19938d3>",
"WARC-IP-Address":"209.188.89.240",
"WARC-Target-URI":"http://afrigeneas.com/newsdata/comments.bak.07182005/?MA",
"WARC-Payload-Digest":"sha1:GFFF32YMBGRFMO65NQQ7BG3G3UGGDQW2",
"WARC-Block-Digest":"sha1:XSFLRTUY2AOVLNM2NE5LCJOIGOCSGZN5",
"WARC-Identified-Payload-Type":"text/html"}

*/
let numRecords = 0;
fs.createReadStream(warcFile)
  .pipe(zlib.createGunzip())
  .pipe(new warc.ReadTransform())
  .on('data', (record) => {
    numRecords++;
    if (numRecords % 100 === 0) {
      // console.log(numRecords);
    }
    if (record.type === 'request') {
      // console.log('request', record);
    }
    else if (record.type === 'response') {
      obj = {
        header: {
          date: record.header['WARC-Date'],
          id: record.header['WARC-Record-ID'],
          "Content-Type": record.header['Content-Type'],
          ip: record.header['WARC-IP-Address'],
          uri: record.header['WARC-Target-URI'],
          type: record.header['WARC-Identified-Payload-Type'],
        },
        body: record.block.body ? record.block.body.toString() : null,
      };
      console.log(JSON.stringify(obj));
      // console.log('response', record.header['WARC-Target-URI']);
      if (record.block.body) {
        // console.log(record.block.body.toString());
      }
    }
    else if (record.type === 'metadata') {
      // console.log('metadata', record);
      // console.log(record.block.fields);
    }
    else {
      // console.log(record.type);
    }
  })
  .on('error', (err) => {
    console.log('Error', err);
    process.exit(-1);
  });

