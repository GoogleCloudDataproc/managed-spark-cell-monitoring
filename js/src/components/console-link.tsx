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

const OPEN_IN_NEW_ICON_PATH =
  'M19 19H5V5h7V3H5a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14c1.1 0 2-.9 2-2v-7h-2v7zM14 3v2h3.59l-9.83 9.83 1.41 1.41L19 6.41V10h2V3h-7z';

/**
 * Returns `url` normalized if it is an absolute https URL, otherwise undefined.
 * Anything else (relative paths, http, javascript:, data:, ...) is rejected so
 * that an untrusted value can never become an executable link.
 */
export function toSafeConsoleUrl(url?: string): string | undefined {
  if (!url) {
    return undefined;
  }
  try {
    const parsed = new URL(url);
    return parsed.protocol === 'https:' ? parsed.href : undefined;
  } catch {
    return undefined;
  }
}

/**
 * Icon-only "View in Google Cloud" link shown in the header's button row,
 * styled like the neighboring tab buttons. Renders nothing until a valid
 * https URL is provided.
 */
export const ConsoleLink = (props: { url?: string; label?: string }) => {
  const href = toSafeConsoleUrl(props.url);
  if (!href) {
    return null;
  }
  const label = props.label ?? 'View in Google Cloud';
  return (
    <a
      className="console-link tabbutton"
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      title={label}
      aria-label={`${label} (opens in a new tab)`}
      // Drop focus once clicked so the link does not keep a focus state
      // after the new tab opens; the neighbouring <span> buttons never hold
      // focus, so this keeps the hover-only circle treatment consistent.
      onClick={(e) => e.currentTarget.blur()}
    >
      <svg width={18} height={18} viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
        <path d={OPEN_IN_NEW_ICON_PATH} />
      </svg>
    </a>
  );
};
