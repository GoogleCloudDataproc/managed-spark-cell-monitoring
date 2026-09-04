/*
 * Copyright 2026 Google LLC
 * Copyright 2017 CERN
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

export const ProgressBar = (props: {
  total?: number;
  running?: number;
  completed?: number;
}) => {
  // Ensure values are valid
  const total = Math.max(props.total ?? 0, 1); // Avoid division by zero
  const completed = Math.max(0, Math.min(props.completed ?? 0, total));
  const running = Math.max(0, Math.min(props.running ?? 0, total - completed));

  // Calculate percentages
  const completedPercent = (completed / total) * 100;
  let runningPercent = (running / total) * 100;

  // Ensure no blue shows when fully completed (handle rounding/edge cases)
  if (completed === total || completedPercent >= 99.99) {
    runningPercent = 0;
  }

  return (
    <div className="tdjobitemprogress cssprogress-container">
      <div className="cssprogress">
        <span
          className="val1"
          style={{
            width: completedPercent + '%',
          }}></span>
        <span
          className="val2"
          style={{
            width: runningPercent + '%',
          }}></span>
      </div>
      <div className="data">
        {props.completed ?? 0}/{props.total ?? 0}
        {running > 0 ? ` (${running})` : ''}
      </div>
    </div>
  );
};
