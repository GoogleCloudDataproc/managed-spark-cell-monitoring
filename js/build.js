/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const esbuild = require('esbuild');
const path = require('path');
const fs = require('fs');

const outDir = path.resolve(__dirname, '../managed_spark_cell_monitoring/static');
if (!fs.existsSync(outDir)) {
  fs.mkdirSync(outDir, { recursive: true });
}

esbuild.build({
  entryPoints: [path.resolve(__dirname, 'src/anywidget-entry.tsx')],
  bundle: true,
  format: 'esm',
  target: 'es2020',
  outfile: path.join(outDir, 'widget.js'),
  loader: {
    '.svg': 'dataurl',
    '.png': 'dataurl',
  },
  minify: process.env.NODE_ENV === 'production',
  sourcemap: true,
}).then(() => {
  console.log('[Managed Spark Cell Monitoring] Frontend bundled successfully to static/widget.js');
}).catch((err) => {
  console.error('[Managed Spark Cell Monitoring] Build failed:', err);
  process.exit(1);
});
