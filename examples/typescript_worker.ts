/**
 * Example: TypeScript worker connecting to a rust-pipe dispatcher
 *
 * Run the Rust dispatcher first, then:
 *   npx tsx examples/typescript_worker.ts
 */
import { createWorker } from '@rust-pipe/worker';

const worker = createWorker({
  url: 'ws://localhost:9876',
  handlers: {
    'scan-target': async (task) => {
      console.log(`Scanning ${task.payload.url} with checks: ${task.payload.checks}`);

      // Simulate security scanning work
      await new Promise((r) => setTimeout(r, 2000));

      return {
        vulnerabilities: [
          {
            type: 'xss',
            severity: 'high',
            url: `${task.payload.url}/search?q=<script>alert(1)</script>`,
            description: 'Reflected XSS in search parameter',
          },
        ],
        scannedEndpoints: 42,
        duration: '2.1s',
      };
    },

    'analyze-code': async (task) => {
      console.log(`Analyzing repo: ${task.payload.repository}`);
      await new Promise((r) => setTimeout(r, 5000));

      return {
        findings: [],
        filesScanned: 156,
      };
    },
  },
  maxConcurrency: 5,
});

worker.start().then(() => {
  console.log('TypeScript worker connected to rust-pipe dispatcher');
});
