import React, {Suspense} from 'react';

const Timeline = React.lazy(
  () => import(/* webpackChunkName: "cellmonitortimeline" */ './timeline'),
);

export const LazyTimeline = () => {
  return (
    <Suspense fallback={<div>loading</div>}>
      <Timeline />
    </Suspense>
  );
};
