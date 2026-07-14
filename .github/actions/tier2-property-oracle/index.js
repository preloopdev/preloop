'use strict';

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

// Workflow command contracts, including GITHUB_STATE and GITHUB_OUTPUT:
// https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-commands
// Metadata and pre/main/post action hooks:
// https://docs.github.com/en/actions/creating-actions/metadata-syntax-for-github-actions
// Pinned official runner v2.335.1 (commit 7d737449ef346f6524f75688d0c9c95fa10ba10a):
// ActionRunner.cs lifecycle dispatch: https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/ActionRunner.cs#L79-L110
// ActionManager.cs preparation/order: https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/ActionManager.cs#L301-L360

const id = process.env.INPUT_ID;
if (id !== 'first' && id !== 'second') {
  throw new Error(`tier2-property-oracle requires INPUT_ID=first or INPUT_ID=second; got ${id || '<unset>'}`);
}

const upperId = id.toUpperCase();
const preStateName = `T2_ORACLE_${upperId}_PRE`;
const mainStateName = `T2_ORACLE_${upperId}_MAIN`;
const preStateEnv = `STATE_${preStateName}`;
const mainStateEnv = `STATE_${mainStateName}`;
const runId = process.env.GITHUB_RUN_ID || 'local';
const runAttempt = process.env.GITHUB_RUN_ATTEMPT || '1';
const eventFile = path.join(
  process.env.RUNNER_TEMP || os.tmpdir(),
  `tier2-property-oracle-${runId}-${runAttempt}.events`,
);

function fail(message) {
  throw new Error(`tier2-property-oracle (${id}): ${message}`);
}

function events() {
  if (!fs.existsSync(eventFile)) return [];
  const content = fs.readFileSync(eventFile, 'utf8');
  return content.split('\n').filter(Boolean);
}

function expectEvents(expected, label) {
  const actual = events();
  if (actual.length !== expected.length || actual.some((entry, index) => entry !== expected[index])) {
    fail(`${label}; expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
}

function appendEvent(phase) {
  fs.appendFileSync(eventFile, `${phase}:${id}\n`, 'utf8');
}

function saveState(name, value) {
  const stateFile = process.env.GITHUB_STATE;
  if (!stateFile) fail(`GITHUB_STATE is unavailable while saving ${name}`);
  fs.appendFileSync(stateFile, `${name}=${value}\n`, 'utf8');
}

function writeOutput(name, value) {
  const outputFile = process.env.GITHUB_OUTPUT;
  if (!outputFile) fail(`GITHUB_OUTPUT is unavailable while writing ${name}`);
  const record = `${name}=${value}`;
  fs.appendFileSync(outputFile, `${record}\n`, 'utf8');
  const records = fs.readFileSync(outputFile, 'utf8').split('\n').filter(Boolean);
  if (!records.includes(record)) fail(`GITHUB_OUTPUT did not retain ${record}`);
}

function runPre() {
  if (process.env[preStateEnv] !== undefined || process.env[mainStateEnv] !== undefined) {
    fail(`pre entered with stale action state (${preStateEnv}/${mainStateEnv})`);
  }
  if (id === 'first') {
    if (events().length !== 0) fail(`first pre found stale event log ${eventFile}`);
  } else {
    expectEvents(['pre:first'], 'second pre must follow first pre in declaration order');
  }
  appendEvent('pre');
  saveState(preStateName, id);
}

function runMain() {
  if (process.env[preStateEnv] !== id) {
    fail(`main cannot see pre state ${preStateEnv}; got ${process.env[preStateEnv] || '<unset>'}`);
  }
  if (process.env[mainStateEnv] !== undefined) {
    fail(`main entered with stale state ${mainStateEnv}`);
  }
  const expected = id === 'first'
    ? ['pre:first', 'pre:second']
    : ['pre:first', 'pre:second', 'main:first'];
  expectEvents(expected, 'main declaration order is not pre-all then main-in-declaration-order');
  appendEvent('main');
  saveState(mainStateName, id);
  writeOutput('result', `tier2-oracle-${id}-main-state-ok`);
}

function runPost() {
  if (process.env[preStateEnv] !== id || process.env[mainStateEnv] !== id) {
    fail(
      `post cannot see both pre and main GITHUB_STATE values; `
      + `${preStateEnv}=${process.env[preStateEnv] || '<unset>'}, `
      + `${mainStateEnv}=${process.env[mainStateEnv] || '<unset>'}`,
    );
  }
  const expectedBeforePost = [
    'pre:first',
    'pre:second',
    'main:first',
    'main:second',
    ...(id === 'first' ? ['post:second'] : []),
  ];
  expectEvents(expectedBeforePost, 'post must run in reverse declaration order after every main');
  appendEvent('post');
  if (id === 'first') {
    expectEvents([
      'pre:first',
      'pre:second',
      'main:first',
      'main:second',
      'post:second',
      'post:first',
    ], 'complete pre/main/post lifecycle ordering');
  }
}

function run() {
  const hasPreState = process.env[preStateEnv] !== undefined;
  const hasMainState = process.env[mainStateEnv] !== undefined;
  if (!hasPreState && !hasMainState) {
    runPre();
  } else if (hasPreState && !hasMainState) {
    runMain();
  } else if (hasPreState && hasMainState) {
    runPost();
  } else {
    fail(`invalid lifecycle state: ${preStateEnv}=${process.env[preStateEnv]}, ${mainStateEnv}=${process.env[mainStateEnv]}`);
  }
}

try {
  run();
} catch (error) {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
}
