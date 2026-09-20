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

import {observer} from 'mobx-react-lite';
import React from 'react';

import {
  DataSet,
  TimelineOptions,
  Timeline as VisTimeline,
} from 'vis-timeline/standalone';
import 'vis-timeline/styles/vis-timeline-graph2d.css';

import {useCellStore, useNotebookStore} from '../store';
import {ErrorBoundary} from './error-boundary';

const timelineOptions: TimelineOptions = {
  margin: {
    item: 2,
    axis: 2,
  },
  stack: true,
  showTooltips: true,
  minHeight: '100px',
  editable: false,
  tooltip: {
    overflowMethod: 'cap',
  },
  align: 'center',
  orientation: 'top',
  verticalScroll: false,
};

const Timeline = observer(() => {
  const notebook = useNotebookStore();
  const cell = useCellStore();

  const timelineDiv = React.useRef<HTMLDivElement>(null);

  const timelineData = [] as any[];
  cell.jobs.forEach((job) => {
    timelineData.push({
      id: job.uniqueId,
      start: job.startTime,
      content: `${job.jobId}:${job.name}`,
      group: 'jobs',
      className: 'job ' + job.status,
      mode: job.status === 'RUNNING' ? 'ongoing' : 'done',
      end: job.endTime ? job.endTime : new Date(),
    });
    job.uniqueStageIds.forEach((uniqueStageId) => {
      const stage = notebook.stages[uniqueStageId];
      if (stage && stage.submissionTime) {
        timelineData.push({
          id: stage.uniqueId,
          start: stage.submissionTime,
          content: `${stage.stageId}:${stage.name}`,
          group: 'stages',
          className: 'stage ' + stage.status,
          mode: stage.status === 'RUNNING' ? 'ongoing' : 'done',
          end: stage.completionTime ? stage.completionTime : new Date(),
        });
      }
    });
  });

  const timelineGroups = React.useMemo(
    () =>
      new DataSet([
        {
          id: 'jobs',
          content: 'Jobs',
          className: 'visjobgroup',
        },
        {id: 'stages', content: 'Stages'},
      ]),
    [],
  );

  const timelineRef = React.useRef<VisTimeline | null>(null);

  React.useEffect(() => {
    if (!timelineDiv.current) {
      return;
    }
    timelineRef.current = new VisTimeline(
      timelineDiv.current,
      timelineData,
      timelineGroups,
      timelineOptions,
    );
    return () => {
      timelineRef.current?.destroy();
    };
  }, []);

  React.useEffect(() => {
    if (timelineRef.current) {
      timelineRef.current.setItems(timelineData);
    }
  }, [timelineData]);
  return (
    <ErrorBoundary>
      <div className="tabcontent">
        <div className="tabcontent-inner">
          <div className="timelinewrapper hidephases">
            <div ref={timelineDiv}></div>
          </div>
        </div>
      </div>
    </ErrorBoundary>
  );
});

export default Timeline;
