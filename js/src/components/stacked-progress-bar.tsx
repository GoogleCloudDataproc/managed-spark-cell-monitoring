/**
 * @license
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

/**
 * Compact three-segment progress track (completed / running / failed) used by
 * the header's active-job strip.
 */
export const StackedProgressBar = (props: {
  total: number;
  completed: number;
  running?: number;
  failed?: number;
  /** Accessible description, e.g. "count: 3 of 8 tasks complete, 2 running". */
  label: string;
  className?: string;
}) => {
  const total = Math.max(props.total || 0, 0);
  const toPercent = (value: number | undefined) => {
    if (total <= 0) {
      return '0%';
    }
    const clamped = Math.min(Math.max(value || 0, 0), total);
    return `${(clamped / total) * 100}%`;
  };

  return (
    <span
      className={'stacked-progress' + (props.className ? ` ${props.className}` : '')}
      role="progressbar"
      aria-valuemin={0}
      aria-valuemax={total}
      aria-valuenow={Math.min(Math.max(props.completed || 0, 0), total)}
      aria-label={props.label}
    >
      <span className="stacked-progress-done" style={{ width: toPercent(props.completed) }} />
      <span className="stacked-progress-running" style={{ width: toPercent(props.running) }} />
      <span className="stacked-progress-failed" style={{ width: toPercent(props.failed) }} />
    </span>
  );
};
