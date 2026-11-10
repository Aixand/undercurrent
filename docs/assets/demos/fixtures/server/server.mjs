// SPDX-License-Identifier: Apache-2.0
// Request faults and dependency availability are separate so error history
// cannot keep an otherwise recovered service unhealthy.
import {createServer} from 'node:http';
import {renameSync, writeFileSync} from 'node:fs';

const port = Number(process.env.PORT ?? 3000);
if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error('Invalid PORT');
const evidence = {
  pid: process.pid, port, dependencyAvailable: true,
  isolatedErrors: 0, dependencyErrors: 0, successfulOrders: 0,
  lastEventAt: Date.now(),
  healthyProbes: 0, unhealthyProbes: 0, stopped: false,
};
const save = () => {
  writeFileSync('server-evidence.json.tmp', JSON.stringify(evidence, null, 2) + '\n');
  renameSync('server-evidence.json.tmp', 'server-evidence.json');
};
const respond = (response, status, body) => {
  response.writeHead(status, {'Content-Type': 'application/json', 'Connection': 'close'});
  response.end(JSON.stringify(body) + '\n');
};
const server = createServer((request, response) => {
  if (request.method === 'GET' && request.url === '/health') {
    evidence[evidence.dependencyAvailable ? 'healthyProbes' : 'unhealthyProbes']++;
    save();
    return respond(response, evidence.dependencyAvailable ? 200 : 503,
      {status: evidence.dependencyAvailable ? 'healthy' : 'unhealthy'});
  }
  if (request.method === 'POST' && request.url === '/demo/fail') {
    evidence.dependencyAvailable = false;
    evidence.lastEventAt = Date.now();
    save();
    console.log('[DEMO] Simulated inventory dependency failure');
    return respond(response, 200, {fault: 'inventory dependency disabled for this demo'});
  }
  if (request.method === 'POST' && request.url === '/demo/recover') {
    evidence.dependencyAvailable = true;
    evidence.lastEventAt = Date.now();
    save();
    console.log('[DEMO] Inventory dependency restored');
    return respond(response, 200, {recovery: 'inventory dependency restored'});
  }
  if (request.method === 'GET' && ['/orders', '/orders?demo_error=1'].includes(request.url)) {
    let status, message;
    if (!evidence.dependencyAvailable) {
      status = 503;
      message = 'inventory dependency unavailable';
      evidence.dependencyErrors++;
    } else if (request.url === '/orders?demo_error=1') {
      status = 500;
      message = 'simulated request failure';
      evidence.isolatedErrors++;
    } else {
      evidence.successfulOrders++;
      save();
      console.log('[INFO] GET /orders -> 200');
      return respond(response, 200, {orders: [{id: 'A100', total: '35.50'}]});
    }
    evidence.lastEventAt = Date.now();
    save();
    console.error(`\x1b[31m[ERROR] GET ${request.url} -> ${status}: ${message}\x1b[0m`);
    return respond(response, status, {error: message});
  }
  respond(response, 404, {error: 'not found'});
});
server.on('error', error => {
  console.error(`[ERROR] Could not start API: ${error.code}`);
  process.exitCode = 1;
});
server.listen(port, '127.0.0.1', () => {
  save();
  console.log(`[INFO] API listening on http://127.0.0.1:${port}`);
  console.log('[INFO] GET /health checks the sample inventory dependency');
});

let closing = false;
function shutdown() {
  if (closing) return;
  closing = true;
  clearTimeout(lifetime);
  evidence.stopped = true;
  save();
  server.close(() => process.exit(0));
  server.closeAllConnections();
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
const lifetime = setTimeout(shutdown, 180_000);
lifetime.unref();
