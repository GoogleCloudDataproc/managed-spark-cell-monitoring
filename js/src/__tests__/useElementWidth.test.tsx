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

import React from 'react';
import { act, render, screen } from '@testing-library/react';
import { useElementWidth } from '../components/use-element-width';

let resizeCallback: (() => void) | undefined;
const originalResizeObserver = window.ResizeObserver;

class MockResizeObserver {
  constructor(callback: () => void) {
    resizeCallback = callback;
  }
  observe() {}
  unobserve() {}
  disconnect() {}
}

const Probe = () => {
  const [ref, width] = useElementWidth<HTMLDivElement>();
  return (
    <div ref={ref} data-testid="probe">
      {width}
    </div>
  );
};

describe('useElementWidth', () => {
  beforeEach(() => {
    resizeCallback = undefined;
    window.ResizeObserver = MockResizeObserver as unknown as typeof ResizeObserver;
  });

  afterEach(() => {
    window.ResizeObserver = originalResizeObserver;
  });

  it('measures the border box both on mount and on resize', () => {
    // An element with padding has a border-box wider than its content box;
    // the hook must report the same box in both code paths.
    const borderBoxWidth = { current: 320.6 };
    const spy = jest
      .spyOn(HTMLElement.prototype, 'getBoundingClientRect')
      .mockImplementation(() => ({ width: borderBoxWidth.current }) as DOMRect);
    try {
      render(<Probe />);
      expect(screen.getByTestId('probe')).toHaveTextContent('320');

      borderBoxWidth.current = 180.2;
      act(() => resizeCallback?.());
      expect(screen.getByTestId('probe')).toHaveTextContent('180');
    } finally {
      spy.mockRestore();
    }
  });
});
