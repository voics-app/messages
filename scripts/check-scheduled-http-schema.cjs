#!/usr/bin/env node
// Validate actual synthetic HTTP responses using the canonical contracts' AJV toolchain.
const { readFileSync } = require('node:fs');
const { resolve, join } = require('node:path');
const { createRequire } = require('node:module');
const [canonical, capture] = process.argv.slice(2);
if (!canonical || !capture) throw new Error('usage: node scripts/check-scheduled-http-schema.cjs <contracts checkout with pnpm install> <SCHEDULED_HTTP_CAPTURE file>');
const canonicalRequire = createRequire(join(resolve(canonical), 'codegen/package.json'));
const Ajv = canonicalRequire('ajv/dist/2020').default;
const formats = canonicalRequire('ajv-formats');
const ajv = new Ajv({ allErrors: true, strict: false });
formats(ajv);
const schema = JSON.parse(readFileSync(join(__dirname, '../crates/contracts/src/scheduled/schema.json'), 'utf8'));
const validate = ajv.compile({ ...schema, $ref: '#/$defs/ScheduledMessageList' });
const responses = JSON.parse(readFileSync(resolve(capture), 'utf8'));
if (responses.length !== 6) throw new Error('expected all six HTTP state captures');
for (const response of responses) {
  if (!validate(response)) throw new Error(JSON.stringify(validate.errors));
  const invalid = structuredClone(response);
  delete invalid.items[0].message;
  if (validate(invalid)) throw new Error('required nullable message was not enforced');
  const wrongState = structuredClone(response);
  wrongState.items[0].message = response.items[0].message === null ? { content: 'must be erased' } : null;
  if (validate(wrongState)) throw new Error('status-dependent draft constraint was not enforced');
}
console.log('Six actual HTTP states conform; twelve missing/null/status counterexamples rejected.');
