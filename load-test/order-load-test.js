// order-load-test.js — reproducible k6 load tests for the order platform.
//
// Targets the REAL API (inspected 2026-10-25 on branch feature/load-testing):
//   POST /api/auth/register {username,email,password}        -> 201 {accessToken,...}
//   POST /api/orders        {items:[{productId,quantity}]}   -> 201 OrderDto (Bearer JWT, role USER)
//   GET  /api/orders/{id}                                     -> 200 OrderDto (owner only)
//   GET  /api/products?size=N                                 -> 200 Page<ProductDto> (public)
//   GET  /actuator/health                                     -> 200 (public)
//
// There is NO client idempotency-key support on POST /api/orders; every
// accepted request creates a new logical order. Do not retry creates.
//
// HTTP 201 means "accepted" only. Completion is asynchronous
// (outbox -> Kafka -> inventory -> payment -> status), so each order is
// polled to a terminal state (COMPLETED / FAILED / CANCELLED) with a bounded
// timeout and reported separately from API acceptance.
//
// Safety: local targets only unless explicitly opted in (see assertTargetSafe).
// Credentials/JWTs are never logged. See load-test/README.md.
import http from 'k6/http';
import { check, fail, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// ---------------------------------------------------------------------------
// Configuration (all via environment; non-secret defaults)
// ---------------------------------------------------------------------------
function envStr(name, def) {
  const v = __ENV[name];
  return v === undefined || v === '' ? def : v;
}
function envInt(name, def) {
  const v = __ENV[name];
  if (v === undefined || v === '') return def;
  const n = parseInt(v, 10);
  if (!isFinite(n)) throw new Error(`Invalid integer for ${name}: "${v}"`);
  return n;
}
function envFloat(name, def) {
  const v = __ENV[name];
  if (v === undefined || v === '') return def;
  const n = parseFloat(v);
  if (!isFinite(n)) throw new Error(`Invalid number for ${name}: "${v}"`);
  return n;
}
function envBool(name, def) {
  const v = __ENV[name];
  if (v === undefined || v === '') return def;
  return v.toLowerCase() === 'true' || v === '1';
}

const BASE_URL = envStr('BASE_URL', 'http://localhost:8080').replace(/\/$/, '');
const PROFILE = envStr('PROFILE', 'smoke').toLowerCase();
const RUN_ID = envStr('RUN_ID', `${Date.now()}`);
const USER_PREFIX = envStr('LOADTEST_USER_PREFIX', 'loadtest');
const USER_PASSWORD = envStr('LOADTEST_USER_PASSWORD', 'LoadTest!123'); // synthetic local users only
const PRODUCT_PREFIX = envStr('LOADTEST_PRODUCT_PREFIX', 'LOADTEST-');
const ORDER_QUANTITY = envInt('ORDER_QUANTITY', 1);
const POLL_COMPLETION = envBool('POLL_COMPLETION', true);
const COMPLETION_TIMEOUT_S = envInt('COMPLETION_TIMEOUT_S', 60);
const POLL_INTERVAL_S = envFloat('POLL_INTERVAL_S', 1);
const THINK_TIME_S = envFloat('THINK_TIME_S', 0);
const HTTP_TIMEOUT = envStr('HTTP_TIMEOUT', '30s');
const MAX_VUS = envInt('MAX_VUS', 60);              // hard safety cap
const MAX_DURATION_S = envInt('MAX_DURATION_S', 1800); // hard safety cap
const ERROR_RATE_MAX = envFloat('ERROR_RATE_MAX', 0.01);
const P95_MS_MAX = envInt('P95_MS_MAX', 5000);
const COMPLETION_RATE_MIN = __ENV.COMPLETION_RATE_MIN ? parseFloat(__ENV.COMPLETION_RATE_MIN) : null;
const SUMMARY_JSON = envStr('SUMMARY_JSON', 'results/summary.json');

// Stage tuning per profile (seconds unless noted).
const LOAD_RAMP_S = envInt('LOAD_RAMP_S', 60);
const LOAD_HOLD_S = envInt('LOAD_HOLD_S', 120);
const STRESS_MAX_VUS = envInt('STRESS_MAX_VUS', 50);
const STRESS_STEP_S = envInt('STRESS_STEP_S', 120);
const SOAK_VUS = envInt('SOAK_VUS', 10);
const SOAK_DURATION = envStr('SOAK_DURATION', '15m');

// ---------------------------------------------------------------------------
// Safety guards (init context — a throw here aborts before any traffic)
// ---------------------------------------------------------------------------
function parseHost(url) {
  const m = url.match(/^https?:\/\/([^\/:?#]+)(?::(\d+))?/i);
  if (!m) throw new Error(`BASE_URL is not a valid http(s) URL: ${url}`);
  return m[1].toLowerCase();
}
function assertTargetSafe(url) {
  const host = parseHost(url);
  if (host.includes('prod')) {
    throw new Error(`Refusing to run: host "${host}" looks like production.`);
  }
  const isLocal =
    host === 'localhost' || host === '[::1]' || host === '::1' ||
    host.startsWith('127.') || host === 'host.docker.internal';
  if (isLocal) return host;
  const allowNonLocal = envBool('LOADTEST_ALLOW_NON_LOCAL', false);
  const allowed = envStr('LOADTEST_ALLOWED_HOSTS', '')
    .split(',').map((h) => h.trim().toLowerCase()).filter(Boolean);
  if (!allowNonLocal || !allowed.includes(host)) {
    throw new Error(
      `Refusing non-local target "${host}". Load tests default to local instances only. ` +
      'To authorize a specific non-local test host set LOADTEST_ALLOW_NON_LOCAL=true and ' +
      `LOADTEST_ALLOWED_HOSTS=${host} (see load-test/README.md).`);
  }
  return host;
}
const TARGET_HOST = assertTargetSafe(BASE_URL);

function durationToSeconds(d) {
  if (typeof d === 'number') return d;
  const m = String(d).match(/(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$/);
  if (!m) throw new Error(`Cannot parse duration "${d}"`);
  return (parseInt(m[1] || 0, 10) * 3600) + (parseInt(m[2] || 0, 10) * 60) + parseInt(m[3] || 0, 10);
}

function buildScenario() {
  switch (PROFILE) {
    case 'smoke':
      return { executor: 'constant-vus', vus: 1, duration: '30s' };
    case 'baseline':
      return { executor: 'constant-vus', vus: 5, duration: '2m' };
    case 'load':
      return {
        executor: 'ramping-vus', startVUs: 0, gracefulRampDown: '30s',
        stages: [
          { duration: `${LOAD_RAMP_S}s`, target: 10 },
          { duration: `${LOAD_HOLD_S}s`, target: 10 },
          { duration: `${LOAD_RAMP_S}s`, target: 25 },
          { duration: `${LOAD_HOLD_S}s`, target: 25 },
          { duration: `${LOAD_RAMP_S}s`, target: 50 },
          { duration: `${LOAD_HOLD_S}s`, target: 50 },
          { duration: '30s', target: 0 },
        ],
      };
    case 'stress': {
      if (!envBool('CONFIRM_STRESS', false)) {
        throw new Error('PROFILE=stress is opt-in only: set CONFIRM_STRESS=true to run it.');
      }
      const stages = [];
      for (let t = 10; t <= STRESS_MAX_VUS; t += 10) {
        stages.push({ duration: `${STRESS_STEP_S}s`, target: t });
      }
      stages.push({ duration: '30s', target: 0 });
      return { executor: 'ramping-vus', startVUs: 0, gracefulRampDown: '30s', stages };
    }
    case 'soak': {
      if (!envBool('CONFIRM_SOAK', false)) {
        throw new Error('PROFILE=soak is opt-in only: set CONFIRM_SOAK=true to run it.');
      }
      const secs = durationToSeconds(SOAK_DURATION);
      if (secs < 15 * 60 || secs > 30 * 60) {
        throw new Error(`SOAK_DURATION must be within 15m..30m (got ${SOAK_DURATION}).`);
      }
      return { executor: 'constant-vus', vus: SOAK_VUS, duration: SOAK_DURATION };
    }
    default:
      throw new Error(`Unknown PROFILE "${PROFILE}" (smoke|baseline|load|stress|soak).`);
  }
}

const scenario = buildScenario();
const peakVUs = scenario.vus || Math.max(...scenario.stages.map((s) => s.target));
const totalSeconds = scenario.duration
  ? durationToSeconds(scenario.duration)
  : scenario.stages.reduce((a, s) => a + durationToSeconds(s.duration), 0);
if (peakVUs > MAX_VUS) {
  throw new Error(`Refusing to run: profile peaks at ${peakVUs} VUs > MAX_VUS=${MAX_VUS}. Raise MAX_VUS explicitly if intended.`);
}
if (totalSeconds > MAX_DURATION_S) {
  throw new Error(`Refusing to run: profile lasts ~${totalSeconds}s > MAX_DURATION_S=${MAX_DURATION_S}. Raise MAX_DURATION_S explicitly if intended.`);
}
if (ORDER_QUANTITY < 1) throw new Error('ORDER_QUANTITY must be >= 1');
if (COMPLETION_TIMEOUT_S < 5) throw new Error('COMPLETION_TIMEOUT_S must be >= 5');

// ---------------------------------------------------------------------------
// Custom metrics
// ---------------------------------------------------------------------------
const ordersAccepted = new Counter('orders_accepted');          // HTTP 201 creates
const ordersCompleted = new Counter('orders_completed');        // final COMPLETED
const ordersFailedTerminal = new Counter('orders_failed_terminal'); // final FAILED
const ordersCancelled = new Counter('orders_cancelled');        // final CANCELLED
const ordersPending = new Counter('orders_pending_at_timeout'); // non-terminal at deadline
const createDuration = new Trend('order_create_duration', true);
const pollDuration = new Trend('order_status_poll_duration', true);
const e2eDuration = new Trend('order_e2e_completion_duration', true);
const completionRate = new Rate('order_completion_rate');       // COMPLETED / accepted (polled)
const terminalRate = new Rate('order_terminal_rate');           // any terminal / accepted (polled)
const httpErrorRate = new Rate('http_error_rate');              // status>=400 or transport error
const status2xx = new Counter('http_status_2xx');
const status4xx = new Counter('http_status_4xx');
const status5xx = new Counter('http_status_5xx');
const httpTimeouts = new Counter('http_timeouts');

const thresholds = {
  http_req_failed: [`rate<${ERROR_RATE_MAX}`],
  http_error_rate: [`rate<${ERROR_RATE_MAX}`],
  order_create_duration: [`p(95)<${P95_MS_MAX}`],
  checks: ['rate>0.99'],
};
if (COMPLETION_RATE_MIN !== null && isFinite(COMPLETION_RATE_MIN)) {
  thresholds.order_completion_rate = [`rate>=${COMPLETION_RATE_MIN}`];
}

export const options = {
  scenarios: { orders: scenario },
  thresholds,
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)', 'count'],
  // Never follow redirects into credential prompts; API is JSON-only.
  maxRedirects: 0,
};

const TERMINAL = ['COMPLETED', 'FAILED', 'CANCELLED'];

function recordHttp(res) {
  if (res.status >= 200 && res.status < 300) status2xx.add(1);
  else if (res.status >= 400 && res.status < 500) status4xx.add(1);
  else if (res.status >= 500) status5xx.add(1);
  if (res.error_code === 1050) httpTimeouts.add(1); // k6: request timeout
  httpErrorRate.add(res.status === 0 || res.status >= 400);
}

function jsonBody(res) {
  try { return res.json(); } catch (e) { return null; }
}

// ---------------------------------------------------------------------------
// setup(): fail fast on bad prerequisites; register synthetic users.
// ---------------------------------------------------------------------------
export function setup() {
  const health = http.get(`${BASE_URL}/actuator/health`, { timeout: HTTP_TIMEOUT, tags: { name: 'health' } });
  if (health.status !== 200) {
    fail(`Prerequisite failed: GET /actuator/health -> ${health.status} at ${BASE_URL}. Is the local stack running?`);
  }

  const prodRes = http.get(`${BASE_URL}/api/products?size=100`, { timeout: HTTP_TIMEOUT, tags: { name: 'list_products' } });
  if (prodRes.status !== 200) fail(`Prerequisite failed: GET /api/products -> ${prodRes.status}`);
  const page = jsonBody(prodRes) || {};
  const all = page.content || [];
  const products = all
    .filter((p) => (p.name || '').startsWith(PRODUCT_PREFIX))
    .map((p) => ({ id: p.id, price: Number(p.price), available: p.availableQuantity }));
  if (products.length === 0) {
    fail(`No products with prefix "${PRODUCT_PREFIX}" found. Seed synthetic data first ` +
      '(load-test/scripts/seed-load-data.sql) — refusing to load-test against catalog data.');
  }
  const seededStock = products.reduce((a, p) => a + (p.available || 0), 0);
  console.log(`setup: ${products.length} load-test products, seeded stock=${seededStock}, target=${TARGET_HOST}, profile=${PROFILE}`);

  // One synthetic user per peak VU, unique to this run. Tokens are kept in
  // memory only and never logged.
  const tokens = [];
  for (let i = 0; i < peakVUs; i++) {
    const username = `${USER_PREFIX}-${RUN_ID}-${i}`;
    const payload = JSON.stringify({
      username,
      email: `${username}@loadtest.local`,
      password: USER_PASSWORD,
    });
    const res = http.post(`${BASE_URL}/api/auth/register`, payload, {
      headers: { 'Content-Type': 'application/json' },
      timeout: HTTP_TIMEOUT,
      tags: { name: 'register' },
    });
    if (res.status !== 200 && res.status !== 201) {
      fail(`Prerequisite failed: register synthetic user -> ${res.status}`);
    }
    const body = jsonBody(res);
    if (!body || !body.accessToken) fail('Prerequisite failed: register response has no accessToken');
    tokens.push(body.accessToken);
  }
  console.log(`setup: registered ${tokens.length} synthetic users for run ${RUN_ID}`);
  return { tokens, products };
}

// ---------------------------------------------------------------------------
// VU iteration: create one order; optionally poll to terminal state.
// ---------------------------------------------------------------------------
export default function (data) {
  const token = data.tokens[(__VU - 1) % data.tokens.length];
  const product = data.products[(__VU + __ITER) % data.products.length];
  const headers = {
    'Content-Type': 'application/json',
    Authorization: `Bearer ${token}`,
  };

  const createRes = http.post(
    `${BASE_URL}/api/orders`,
    JSON.stringify({ items: [{ productId: product.id, quantity: ORDER_QUANTITY }] }),
    { headers, timeout: HTTP_TIMEOUT, tags: { name: 'create_order' } },
  );
  recordHttp(createRes);
  if (createRes.status > 0) createDuration.add(createRes.timings.duration);

  const created = jsonBody(createRes);
  const expectedTotal = product.price * ORDER_QUANTITY;
  const okCreate = check(createRes, {
    'create: status 201': (r) => r.status === 201,
    'create: has id': () => created !== null && created.id !== undefined,
    'create: status CREATED': () => created !== null && created.status === 'CREATED',
    'create: total matches price*qty': () =>
      created !== null && Math.abs(Number(created.totalAmount) - expectedTotal) < 0.01,
  });
  if (!okCreate || !created || created.id === undefined) {
    if (THINK_TIME_S > 0) sleep(THINK_TIME_S);
    return;
  }
  ordersAccepted.add(1);

  if (POLL_COMPLETION) {
    const orderId = created.id;
    const deadline = Date.now() + COMPLETION_TIMEOUT_S * 1000;
    const started = Date.now();
    let finalStatus = null;
    while (Date.now() < deadline) {
      sleep(POLL_INTERVAL_S);
      const pollRes = http.get(`${BASE_URL}/api/orders/${orderId}`, {
        headers, timeout: HTTP_TIMEOUT, tags: { name: 'poll_order' },
      });
      recordHttp(pollRes);
      if (pollRes.status > 0) pollDuration.add(pollRes.timings.duration);
      const okPoll = check(pollRes, { 'poll: status 200': (r) => r.status === 200 });
      if (!okPoll) break;
      const body = jsonBody(pollRes);
      if (body && TERMINAL.includes(body.status)) { finalStatus = body.status; break; }
    }
    if (finalStatus !== null) {
      e2eDuration.add(Date.now() - started);
      if (finalStatus === 'COMPLETED') ordersCompleted.add(1);
      else if (finalStatus === 'FAILED') ordersFailedTerminal.add(1);
      else ordersCancelled.add(1);
      completionRate.add(finalStatus === 'COMPLETED');
      terminalRate.add(true);
    } else {
      ordersPending.add(1);
      completionRate.add(false);
      terminalRate.add(false);
    }
  }
  if (THINK_TIME_S > 0) sleep(THINK_TIME_S);
}

export function teardown(data) {
  // Data is intentionally left in the local test DB for post-run verification
  // (scripts/verify-consistency.sql). Cleanup is an explicit, guarded manual
  // step (scripts/cleanup-load-data.sql) — never implicit.
  console.log(`teardown: run ${RUN_ID} finished. Verify DB state before cleaning up synthetic data.`);
}

// ---------------------------------------------------------------------------
// Summary: machine-readable JSON + concise stdout (no external jslib deps).
// ---------------------------------------------------------------------------
function metric(data, name) {
  const m = data.metrics[name];
  return m ? m.values : {};
}
function trend(data, name) {
  const v = metric(data, name);
  return {
    count: v.count || 0, avg_ms: v.avg || 0, min_ms: v.min || 0,
    p50_ms: v.med || 0, p90_ms: v['p(90)'] || 0, p95_ms: v['p(95)'] || 0,
    p99_ms: v['p(99)'] || 0, max_ms: v.max || 0,
  };
}

export function handleSummary(data) {
  const durationS = data.state ? data.state.testRunDurationMs / 1000 : 0;
  const accepted = metric(data, 'orders_accepted').count || 0;
  const summary = {
    run_id: RUN_ID,
    profile: PROFILE,
    target_host: TARGET_HOST,
    started_from_env: {
      base_url: BASE_URL, poll_completion: POLL_COMPLETION,
      completion_timeout_s: COMPLETION_TIMEOUT_S, order_quantity: ORDER_QUANTITY,
      peak_vus_configured: peakVUs, planned_duration_s: totalSeconds,
    },
    http: {
      requests: metric(data, 'http_reqs'),
      duration: trend(data, 'http_req_duration'),
      failed_rate: (metric(data, 'http_req_failed') || {}).rate || 0,
      error_rate_custom: (metric(data, 'http_error_rate') || {}).rate || 0,
      status_2xx: metric(data, 'http_status_2xx').count || 0,
      status_4xx: metric(data, 'http_status_4xx').count || 0,
      status_5xx: metric(data, 'http_status_5xx').count || 0,
      timeouts: metric(data, 'http_timeouts').count || 0,
    },
    checks: metric(data, 'checks'),
    order_creation: {
      accepted,
      accepted_per_s: durationS > 0 ? accepted / durationS : 0,
      latency: trend(data, 'order_create_duration'),
    },
    order_completion: {
      polled: POLL_COMPLETION,
      completed: metric(data, 'orders_completed').count || 0,
      failed_terminal: metric(data, 'orders_failed_terminal').count || 0,
      cancelled: metric(data, 'orders_cancelled').count || 0,
      pending_at_timeout: metric(data, 'orders_pending_at_timeout').count || 0,
      completion_rate: (metric(data, 'order_completion_rate') || {}).rate || 0,
      terminal_rate: (metric(data, 'order_terminal_rate') || {}).rate || 0,
      e2e_latency: trend(data, 'order_e2e_completion_duration'),
      poll_latency: trend(data, 'order_status_poll_duration'),
    },
    // Note: HTTP acceptance != asynchronous completion; see README.
    test_run_duration_s: durationS,
  };
  const stdoutText =
    `\n=== load-test summary (profile=${PROFILE}, run=${RUN_ID}) ===\n` +
    `http requests: ${summary.http.requests.count || 0} (${(summary.http.requests.rate || 0).toFixed(2)}/s), ` +
    `error rate ${(summary.http.error_rate_custom * 100).toFixed(2)}%, timeouts ${summary.http.timeouts}\n` +
    `create latency p50=${summary.order_creation.latency.p50_ms.toFixed(1)}ms ` +
    `p95=${summary.order_creation.latency.p95_ms.toFixed(1)}ms p99=${summary.order_creation.latency.p99_ms.toFixed(1)}ms\n` +
    `orders accepted: ${accepted} (${summary.order_creation.accepted_per_s.toFixed(2)}/s)\n` +
    (POLL_COMPLETION
      ? `completed: ${summary.order_completion.completed}, failed(terminal): ${summary.order_completion.failed_terminal}, ` +
        `pending@timeout: ${summary.order_completion.pending_at_timeout}, completion rate ` +
        `${(summary.order_completion.completion_rate * 100).toFixed(1)}%, ` +
        `e2e p50=${summary.order_completion.e2e_latency.p50_ms.toFixed(0)}ms p95=${summary.order_completion.e2e_latency.p95_ms.toFixed(0)}ms\n`
      : 'completion polling disabled (acceptance-only run)\n');
  const out = { stdout: stdoutText };
  out[SUMMARY_JSON] = JSON.stringify(summary, null, 2);
  return out;
}
