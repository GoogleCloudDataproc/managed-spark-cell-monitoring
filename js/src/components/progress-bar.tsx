/**
 * @license
 * Copyright 2025 Google LLC
 */

import React from 'react';

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
        {props.completed}/{props.total}
        {props.running > 0 ? ` (${props.running})` : ''}
      </div>
    </div>
  );
};
