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

import {observer} from 'mobx-react-lite';
import Plotly from 'plotly.js-basic-dist';
import React from 'react';
import {useCellStore} from '../store';

import createPlotlyComponent from 'react-plotly.js/factory';
import {ErrorBoundary} from './error-boundary';
const Plot = createPlotlyComponent(Plotly);

/**
 * TaskChart renders a time-series chart of Spark task scheduling:
 * - "Running Tasks" (green): tasks currently running, capped at executor cores.
 * - "Scheduled Tasks" (orange): tasks waiting for resources, above executor cores.
 * - "Executor Cores" (blue): available executor slots over time.
 * - Vertical lines and markers for job boundaries.
 * - Custom legend and theme-aware colors.
 */

// --- Theme and Layout Helpers ---
const isDarkMode = (): boolean => {
  // Check for JupyterLab dark theme
  const jupyterElement = document.querySelector(
    '[data-jp-theme-light="false"]',
  );
  if (jupyterElement) {
    return true;
  }

  // Check for VSCode dark theme
  const vscodeElement = document.querySelector(
    '.vscode-dark, .vscode-high-contrast',
  );
  if (vscodeElement) {
    return true;
  }
  return false;
};

// Helper to get CSS variable
function getCssVar(name: string) {
  return getComputedStyle(document.documentElement)
    .getPropertyValue(name)
    ?.trim();
}

const getPlotDefaultLayout = (): Partial<Plotly.Layout> => {
  const darkMode = isDarkMode();
  const paperBg = getCssVar(
    darkMode ? '--cellmonitor-bg-dark' : '--cellmonitor-bg-light',
  );
  const gridColor = getCssVar('--cellmonitor-grid-dark');
  const textColor = getCssVar(
    darkMode ? '--cellmonitor-text-dark' : '--cellmonitor-text-light',
  );
  return {
    showlegend: true,
    paper_bgcolor: paperBg,
    plot_bgcolor: paperBg,
    margin: {
      t: 50,
      l: 30,
      r: 30,
      b: 60,
    },
    xaxis: {
      type: 'date',
      showticklabels: true,
      tickformat: '%H:%M:%S.%f',
      title: {
        text: '',
      },
      tickfont: {
        color: textColor,
      },
      gridcolor: gridColor,
      fixedrange: false,
      autorange: true,
    },
    yaxis: {
      fixedrange: true,
      tickfont: {
        color: textColor,
      },
      gridcolor: gridColor,
    },
    dragmode: 'pan',
    shapes: [],
    legend: {
      orientation: 'h',
      x: 1,
      xanchor: 'right',
      y: 1.08,
      yanchor: 'top',
      font: {
        family: 'sans-serif',
        size: 12,
        color: textColor,
      },
      itemsizing: 'trace',
      tracegroupgap: 5,
      itemclick: 'toggle',
      itemdoubleclick: 'toggleothers',
    },
  };
};

// --- Trace Creators ---
function createRunningTasksTrace(
  taskDataX: number[],
  taskDataY: number[],
  executorDataY: number[],
): Plotly.Data {
  const color = getCssVar('--cellmonitor-running-tasks');
  const fill = getCssVar('--cellmonitor-running-tasks-fill');
  return {
    x: taskDataX,
    y: taskDataY.map((numTasks, index) =>
      Math.min(numTasks, executorDataY[index] || 0),
    ),
    type: 'scatter',
    mode: 'lines',
    line: {color, width: 2, shape: 'hv'},
    fill: 'tozeroy',
    fillcolor: fill,
    name: 'Running Tasks',
    legendgroup: 'running',
    showlegend: false,
  };
}

type ScheduledTasksData = {
  scheduledX: number[];
  scheduledY: number[];
  baseX: number[];
  baseY: number[];
};

function createScheduledTasksData(
  taskDataX: number[],
  taskDataY: number[],
  executorDataY: number[],
): ScheduledTasksData {
  const scheduledX: number[] = [];
  const scheduledY: number[] = [];
  const baseX: number[] = [];
  const baseY: number[] = [];
  let inScheduledRegion = false;

  function pushAll(x: number, y1: number, y2: number) {
    scheduledX.push(x);
    scheduledY.push(y1);
    baseX.push(x);
    baseY.push(y2);
  }

  for (let i = 0; i < taskDataX.length; i++) {
    const time = taskDataX[i];
    const numTasks = taskDataY[i];
    const numCores = executorDataY[i] || 0;
    const scheduledTasks = Math.max(0, numTasks - numCores);

    if (scheduledTasks === 0) {
      if (inScheduledRegion) {
        pushAll(time, numCores, numCores);
        inScheduledRegion = false;
      }
      continue;
    }

    if (!inScheduledRegion && i > 0) {
      pushAll(
        taskDataX[i - 1],
        executorDataY[i - 1] || 0,
        executorDataY[i - 1] || 0,
      );
      inScheduledRegion = true;
    }
    pushAll(time, numTasks, numCores);
  }
  return {scheduledX, scheduledY, baseX, baseY};
}

function createScheduledBaseTrace(
  baseX: number[],
  baseY: number[],
): Plotly.Data {
  return {
    x: baseX,
    y: baseY,
    type: 'scatter',
    mode: 'lines',
    line: {color: 'transparent', width: 0},
    name: 'Scheduled Base',
    showlegend: false,
    hoverinfo: 'skip',
  };
}

function createScheduledTasksTrace(
  scheduledX: number[],
  scheduledY: number[],
): Plotly.Data {
  const color = getCssVar('--cellmonitor-scheduled-tasks');
  const fill = getCssVar('--cellmonitor-scheduled-tasks-fill');
  return {
    x: scheduledX,
    y: scheduledY,
    type: 'scatter',
    mode: 'lines',
    line: {color, width: 2, shape: 'hv'},
    fill: 'tonexty',
    fillcolor: fill,
    name: 'Scheduled Tasks',
    legendgroup: 'scheduled',
    showlegend: false,
  };
}

function createExecutorTrace(
  executorDataX: number[],
  executorDataY: number[],
): Plotly.Data {
  const color = getCssVar('--cellmonitor-executor-cores');
  return {
    x: executorDataX,
    y: executorDataY,
    type: 'scatter',
    mode: 'lines',
    line: {color, width: 2, shape: 'hv'},
    name: 'Executor Cores',
    legendgroup: 'executors',
    showlegend: false,
  };
}

function createJobTrace(
  jobDataX: number[],
  jobDataY: number[],
  jobDataText: string[],
): Plotly.Data {
  const color = getCssVar('--cellmonitor-job-marker');
  return {
    x: jobDataX,
    y: jobDataY,
    text: jobDataText,
    type: 'scatter',
    mode: 'markers',
    showlegend: false,
    marker: {symbol: 23, color, size: 1},
  };
}

function createLegendTrace(
  name: string,
  color: string,
  legendgroup: string,
): Plotly.Data {
  const colorVal = getCssVar(color); // color is now a CSS var name
  return {
    x: [null],
    y: [null],
    type: 'scatter',
    mode: 'markers',
    marker: {symbol: 'circle', size: 8, color: colorVal},
    name,
    legendgroup,
    showlegend: true,
  };
}

// --- Main Component ---
const plotOptions = {displaylogo: false, scrollZoom: true};

const TaskChart = observer(() => {
  const cell = useCellStore();
  const taskChartStore = cell.taskChartStore;

  const [chartRefreshRevision, setRevision] = React.useState(1);
  const [themeRevision, setThemeRevision] = React.useState(1);

  // --- Data Preparation ---
  const data = React.useMemo(() => {
    const {scheduledX, scheduledY, baseX, baseY} = createScheduledTasksData(
      taskChartStore.taskDataX,
      taskChartStore.taskDataY,
      taskChartStore.executorDataY,
    );

    return [
      createRunningTasksTrace(
        taskChartStore.taskDataX,
        taskChartStore.taskDataY,
        taskChartStore.executorDataY,
      ),
      createScheduledBaseTrace(baseX, baseY),
      createScheduledTasksTrace(scheduledX, scheduledY),
      createExecutorTrace(
        taskChartStore.executorDataX,
        taskChartStore.executorDataY,
      ),
      createJobTrace(
        taskChartStore.jobDataX,
        taskChartStore.jobDataY,
        taskChartStore.jobDataText,
      ),
      createLegendTrace(
        'Running Tasks',
        '--cellmonitor-running-tasks',
        'running',
      ),
      createLegendTrace(
        'Scheduled Tasks',
        '--cellmonitor-scheduled-tasks',
        'scheduled',
      ),
      createLegendTrace(
        'Executor Cores',
        '--cellmonitor-executor-cores',
        'executors',
      ),
    ];
  }, [
    taskChartStore.taskDataX,
    taskChartStore.taskDataY,
    taskChartStore.executorDataX,
    taskChartStore.executorDataY,
    taskChartStore.jobDataX,
    taskChartStore.jobDataY,
    taskChartStore.jobDataText,
  ]);

  const plotLayout: Partial<Plotly.Layout> = React.useMemo(() => {
    const darkMode = isDarkMode();
    const jobLineColor = getCssVar('--cellmonitor-job-marker');
    const textColor = getCssVar(
      darkMode ? '--cellmonitor-text-dark' : '--cellmonitor-text-light',
    );
    return {
      ...getPlotDefaultLayout(),
      xaxis: {
        ...getPlotDefaultLayout().xaxis,
        range:
          taskChartStore.taskDataX.length > 0
            ? [
                taskChartStore.taskDataX[0],
                taskChartStore.taskDataX[taskChartStore.taskDataX.length - 1],
              ]
            : undefined,
      },
      shapes: taskChartStore.jobDataX.map((job) => {
        return {
          type: 'line',
          yref: 'paper',
          x0: job,
          y0: 0,
          x1: job,
          y1: 1,
          line: {
            color: jobLineColor,
            width: 1.5,
          },
        };
      }),
      annotations: [
        {
          text: new Date().toLocaleDateString('en-US', {
            day: 'numeric',
            month: 'long',
            year: 'numeric',
          }),
          x: 0,
          y: 1.08,
          xref: 'paper',
          yref: 'paper',
          xanchor: 'left',
          yanchor: 'top',
          showarrow: false,
          font: {
            family: 'Roboto',
            size: 12,
            color: textColor,
          },
        },
      ],
      datarevision: chartRefreshRevision,
      uirevision: 'preserve_zoom',
    };
  }, [taskChartStore.jobDataX, chartRefreshRevision, themeRevision]);

  // Listen for theme changes
  React.useEffect(() => {
    const handleThemeChange = () => {
      setThemeRevision((prev) => prev + 1);
    };

    // Listen for system theme changes
    const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
    mediaQuery.addEventListener('change', handleThemeChange);

    // Listen for DOM changes that might indicate theme changes
    const observer = new MutationObserver((mutations) => {
      mutations.forEach((mutation) => {
        if (
          mutation.type === 'attributes' &&
          (mutation.attributeName === 'data-jp-theme-light' ||
            mutation.attributeName === 'class')
        ) {
          handleThemeChange();
        }
      });
    });

    // Observe the document body for class changes (VSCode theme changes)
    observer.observe(document.body, {
      attributes: true,
      attributeFilter: ['class', 'data-jp-theme-light'],
      subtree: true,
    });

    // Observe the document element for JupyterLab theme changes
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ['data-jp-theme-light'],
      subtree: true,
    });

    return () => {
      mediaQuery.removeEventListener('change', handleThemeChange);
      observer.disconnect();
    };
  }, []);

  // Periodically refresh the chart by updating the revision
  React.useEffect(() => {
    const refreshInterval = setInterval(() => {
      setRevision((revision) => revision + 1);
    }, 2000);
    return () => {
      // clean up when react component is unmounted.
      clearInterval(refreshInterval);
    };
  }, []);

  const containerRef = React.useRef<HTMLDivElement>(null);
  const [dimensions, setDimensions] = React.useState({ width: 0, height: 0 });

  React.useEffect(() => {
    if (!containerRef.current) return;
    const observer = new ResizeObserver((entries) => {
      const { width, height } = entries[0].contentRect;
      if (width > 0 && height > 0) {
        setDimensions({ width, height });
      }
    });
    observer.observe(containerRef.current);
    return () => observer.disconnect();
  }, []);

  // Inject dimensions manually into layout to bypass buggy Plotly internal resizer
  const finalLayout = React.useMemo(() => {
    return {
      ...plotLayout,
      width: dimensions.width,
      height: dimensions.height,
    };
  }, [plotLayout, dimensions.width, dimensions.height]);

  return (
    <ErrorBoundary>
      <div className="tabcontent">
        <div className="tabcontent-inner" ref={containerRef} style={{ width: '100%', height: '100%', minHeight: '300px' }}>
          {dimensions.width > 0 && dimensions.height > 0 && (
            <Plot
              layout={finalLayout}
              data={data}
              config={plotOptions}
              useResizeHandler={false}
              style={{width: '100%', height: '100%'}}
              revision={chartRefreshRevision}
            />
          )}
        </div>
      </div>
    </ErrorBoundary>
  );
});
export default TaskChart;
