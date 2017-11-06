const _ = require('lodash');
const { Transform } = require('stream');
const HTTPParser = require('http-parser-js').HTTPParser;

const RECORD_START = new Buffer('WARC/1.0\r\n');
const CR = 13;
const LF = 10;
const SP = 32;
const HT = 9;
const INIT = 1;
const READ_HEADER = 2;
const READ_BLOCK = 3;

function moreData(state, spaceNeeded) {
  const curBuf = state.chunk.slice(state.chunkPos);
  return _.merge(state, {
    partial: { curBuf, spaceNeeded },
    chunkPos: state.chunk.length,
  });
}

function parseRecordStart(state) {
  const bufferSize = state.chunk.length - state.chunkPos;
  if (bufferSize < RECORD_START.length) {
    // not enough data to match on record start
    return moreData(state, RECORD_START.length);
  }

  const endPos = state.chunkPos + RECORD_START.length;
  const possibleMatch = state.chunk.slice(state.chunkPos, endPos);
  if (possibleMatch.equals(RECORD_START)) {
    return _.merge(state, {
      partial: {
        curJob: READ_HEADER,
      },
      chunkPos: endPos,
    });
  }

  throw new Error('Missing record start marker');
}

function parseField(chunk, chunkPos) {
  let pos = chunkPos;
  let fieldName = null;
  const colonCharCode = ':'.charCodeAt(0);
  while (pos < chunk.length) {
    const charCode = chunk[pos];
    if (charCode === colonCharCode) {
      // managed to parse the fieldname
      fieldName = chunk.slice(chunkPos, pos).toString();
      pos++;
      break;
    }
    pos++;
  }

  if (!fieldName) {
    // not enough data to read fieldName
    return [pos - chunkPos, null];
  }

  // skip whitespace
  do {
    const charCode = chunk[pos];
    if (charCode === SP || charCode === HT) {
      pos++;
    } else break;
  } while (pos < chunk.length);

  // find uncontinued CRLF
  let endPos = null;
  const startPos = pos;
  for (;pos < (chunk.length - 3); pos++) {
    const c1 = chunk[pos];
    const c2 = chunk[pos + 1];
    const c3 = chunk[pos + 2];
    if (c1 === CR && c2 === LF && (c3 !== SP && c3 !== HT)) {
      endPos = pos;
      pos += 2;
      break;
    }
  }

  if (!endPos) {
    // not enough data to read value
    return [pos - chunkPos, null];
  }

  const fieldValue = chunk.slice(startPos, endPos).toString('utf8');

  return [pos - chunkPos, { [fieldName]: fieldValue }];
}

function isHeaderEnd(chunk, chunkPos) {
  if ((chunk.length - chunkPos) < 2) return false;
  const c1 = chunk[chunkPos];
  const c2 = chunk[chunkPos + 1];

  return (c1 === CR && c2 === LF);
}

function parseHeader(state) {
  let fields = _.get(state, 'partial.fields', {});
  while (!isHeaderEnd(state.chunk, state.chunkPos)) {
    const [bytesConsumed, field] = parseField(state.chunk, state.chunkPos);
    if (!field) {
      // not enough data to read field
      return _.merge(moreData(state), { partial: { fields } });
    }

    fields = _.merge(fields, field);
    state.chunkPos += bytesConsumed;
  }

  state.chunkPos += 2;

  fields['Content-Length'] = parseInt(_.get(fields, 'Content-Length', -1), 10);

  delete state.partial.fields;
  return _.merge(state, {
    partial: {
      header: fields,
      curJob: READ_BLOCK,
    },
  });
}

function parseResponse(chunk, chunkPos, length) {
  const parser = new HTTPParser(HTTPParser.RESPONSE);
  let headers = null;
  let body = null;
  parser.onHeadersComplete = (res) => {
    if (res.headers.length % 2 === 1) {
      // eslint-disable-next-line no-console
      console.error('Warning: unexpected uneven headers array');
      res.headers.pop();
    }

    headers = _.fromPairs(_.chunk(res.headers, 2));
  };
  parser.onBody = (bodyBuf, offset, len) => {
    body = bodyBuf.slice(offset, offset + len);
  };
  parser.execute(chunk.slice(chunkPos, chunkPos + length));
  return { headers, body };
}

function parseRequest(chunk, chunkPos, length) {
  const parser = new HTTPParser(HTTPParser.REQUEST);
  let info = null;
  let headers = null;
  let body = null;
  parser.onHeadersComplete = (res) => {
    if (res.headers.length % 2 === 1) {
      // eslint-disable-next-line no-console
      console.error('Warning: unexpected uneven headers array');
      res.headers.pop();
    }

    headers = _.fromPairs(_.chunk(res.headers, 2));
    info = _.omit(res, 'headers');
  };
  parser.onBody = (bodyBuf, offset, len) => {
    body = bodyBuf.slice(offset, offset + len);
  };
  parser.execute(chunk.slice(chunkPos, chunkPos + length));
  return { info, headers, body };
}

function parseCustomFields(chunk, chunkPos, length) {
  const str = chunk.slice(chunkPos, (chunkPos + length) - 4).toString();
  const lineSplit = (str.indexOf('\r\n') > 0) ? '\r\n' : '\n';
  const fields = str.split(lineSplit).map((line) => {
    const parts = line.split(':');
    const name = parts[0];
    const value = parts.slice(1).join(':').trim();
    return [name, value];
  });

  return { fields: _.fromPairs(fields) };
}

function parseInfo(chunk, chunkPos, length) {
  return parseCustomFields(chunk, chunkPos, length);
}

function parseMetadata(chunk, chunkPos, length) {
  return parseCustomFields(chunk, chunkPos, length);
}

function parseResource(chunk, chunkPos, length) {
  const str = chunk.slice(chunkPos, (chunkPos + length) - 2).toString();

  console.log(str);
  throw new Error('TODO');
}

function nextRecord(state, blockSize) {
  if (!state) throw new Error('missing state');
  if (!blockSize) throw new Error('missing blockSize');

  state.chunkPos += blockSize;
  delete state.partial.header;
  return _.merge(state, {
    partial: {
      curJob: INIT,
    },
  });
}

function addRecord(state, record) {
  if (!state) throw new Error('missing state');
  if (!record) throw new Error('missing record');

  const curRecords = _.get(state, 'records', []);
  return _.merge(state, {
    records: curRecords.concat([record]),
  });
}

function parseBlock(state) {
  const type = _.get(state, 'partial.header.WARC-Type');
  if (!type) throw new Error('Missing WARC-Type in header');

  const length = _.get(state, 'partial.header.Content-Length');
  if (!length) throw new Error('Missing Content-Length in header');

  const blockSize = length + 4; // 4 = CRLFCRLF

  if (blockSize > (state.chunk.length - state.chunkPos)) {
    // not enough data to parse
    return moreData(state, blockSize);
  }

  switch (type) {
    case 'warcinfo': {
      const infoRecord = parseInfo(state.chunk, state.chunkPos, length);
      const record = {
        type: 'warcinfo',
        header: _.get(state, 'partial.header'),
        block: infoRecord,
      };
      return addRecord(nextRecord(state, blockSize), record);
    }
    case 'request': {
      const requestRecord = parseRequest(state.chunk, state.chunkPos, length);
      const record = {
        type: 'request',
        header: _.get(state, 'partial.header'),
        block: requestRecord,
      };
      return addRecord(nextRecord(state, blockSize), record);
    }
    case 'response': {
      const responseRecord = parseResponse(state.chunk, state.chunkPos, length);
      const record = {
        type: 'response',
        header: _.get(state, 'partial.header'),
        block: responseRecord,
      };
      return addRecord(nextRecord(state, blockSize), record);
    }
    case 'metadata': {
      const metadataRecord = parseMetadata(state.chunk, state.chunkPos, length);
      const record = {
        type: 'metadata',
        header: _.get(state, 'partial.header'),
        block: metadataRecord,
      };
      return addRecord(nextRecord(state, blockSize), record);
    }
    case 'resource': {
      console.log(state);
      const resourceRecord = parseResource(state.chunk, state.chunkPos, length);
      const record = {
        type: 'metadata',
        header: _.get(state, 'partial.header'),
        block: resourceRecord,
      };
      return addRecord(nextRecord(state, blockSize), record);
    }
    default:
      // eslint-disable-next-line no-console
      console.error(`Unknown record type: ${type}`);
      if ((state.chunk.length - state.chunkPos) >= blockSize) {
        return nextRecord(state, blockSize);
      }

      throw new Error('TODO');
  }
}

function parseChunk(state) {
  const curJob = _.get(state, 'partial.curJob', INIT);
  switch (curJob) {
    case INIT:
      return parseRecordStart(state);
    case READ_HEADER:
      return parseHeader(state);
    case READ_BLOCK:
      return parseBlock(state);
    default:
      throw new Error('unknown job to do');
  }
}


function parseWarc(state, chunk) {
  const curBuf = _.get(state, 'partial.curBuf', new Buffer(0));
  if (curBuf.length > 0) {
    const spaceNeeded = _.get(state, 'partial.spaceNeeded');
    if (spaceNeeded) {
      const pendingSize = _.get(state, 'partial.pendingSize', 0);
      if (spaceNeeded > (curBuf.length + pendingSize + chunk.length)) {
        if (!state.partial.pending) state.partial.pending = [];

        state.partial.pending.push(chunk);
        state.partial.pendingSize = pendingSize + chunk.length;
        return state;
      }
    }
    const pending = _.get(state, 'partial.pending', []);
    state.chunk = Buffer.concat([curBuf].concat(pending).concat(chunk));
    delete state.partial.curBuf;
    delete state.partial.pending;
    delete state.partial.spaceNeeded;
    delete state.partial.pendingSize;
  } else {
    state.chunk = chunk;
  }
  state.chunkPos = 0;

  while (state.chunkPos < state.chunk.length) {
    state = parseChunk(state);
  }

  return state;
}

class WarcRead extends Transform {
  constructor(options) {
    super(_.merge(options, { readableObjectMode: true }));

    this.state = { chunk: new Buffer(0), chunkPos: 0 };
  }

  _transform(chunk, encoding, callback) {
    this.state = parseWarc(this.state, chunk);
    _.each(_.get(this.state, 'records', []), (record) => this.push(record));
    this.state.records = [];
    callback(null);
  }
}

module.exports = {
  ReadTransform: WarcRead,
};
