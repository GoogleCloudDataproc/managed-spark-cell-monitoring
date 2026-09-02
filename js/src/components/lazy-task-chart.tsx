import React, {Suspense} from 'react';

const TaskChart = React.lazy(
  () => import(/* webpackChunkName: "cellmonitortaskchart" */ './task-chart'),
);

export const LazyTaskChart = () => {
  return (
    <Suspense fallback={<div>loading</div>}>
      <TaskChart />
    </Suspense>
  );
};
