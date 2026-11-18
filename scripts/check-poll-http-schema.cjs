#!/usr/bin/env node
// Check actual HTTP/event captures against the full canonical contract bundle.
const { readFileSync } = require('node:fs');
const { resolve, join } = require('node:path');
const { createRequire } = require('node:module');
const [canonical, capture] = process.argv.slice(2);
if (!canonical || !capture) throw new Error('usage: node scripts/check-poll-http-schema.cjs <contracts checkout> <POLL_HTTP_CAPTURE file>');
const canonicalRequire = createRequire(join(resolve(canonical), 'codegen/package.json'));
const Ajv = canonicalRequire('ajv/dist/2020').default;
const formats = canonicalRequire('ajv-formats');
const ajv = new Ajv({ allErrors: true, strict: false });
formats(ajv);
const schema = JSON.parse(readFileSync(join(resolve(canonical), 'dist/schemas.bundle.json'), 'utf8'));
const responses = JSON.parse(readFileSync(resolve(capture), 'utf8'));
if (responses.length !== 11) throw new Error('expected six REST responses and five poll events');
let rejected = 0;
for (const response of responses) {
  const validate = ajv.compile({ ...schema, $id: undefined, $ref: `#/$defs/${response.schema}` });
  if (!validate(response.data)) throw new Error(`${response.schema}: ${JSON.stringify(validate.errors)}`);
  const invalid = structuredClone(response.data);
  if (response.schema === 'PollSnapshot') {
    delete invalid.results.vote_version;
  } else if (response.schema.startsWith('MessagePollVote')) {
    delete invalid.vote_version;
  } else {
    delete invalid.poll.question;
  }
  if (validate(invalid)) throw new Error(`${response.schema}: required poll field was not enforced`);
  rejected++;
}
console.log(`${responses.length} actual HTTP/event captures conform; ${rejected} missing-field counterexamples rejected.`);
