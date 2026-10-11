# -*- coding: utf-8 -*-
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""IPython kernel extension for Managed Spark Cell Monitoring."""

import json
import logging
import os
import re
import socket
import threading
import urllib.parse
import urllib.request
import uuid

import IPython.display
import managed_spark_cell_monitoring as cell_monitoring
from managed_spark_cell_monitoring.widget import ManagedSparkCellWidget

ipykernel_imported = True
spark_imported = True

# These fail-safes gracefully disable the extension without crashing the kernel
# if it is loaded in a standard Jupyter environment where Spark is not
# installed.
try:
  from ipykernel import zmqshell  # pylint: disable=g-import-not-at-top
except ImportError:
  ipykernel_imported = False

try:
  from pyspark import SparkConf  # pylint: disable=g-import-not-at-top
except ImportError:
  spark_imported = False

logger = logging.getLogger(__name__)

_METADATA_IP_BASE = 'http://169.254.169.254/computeMetadata/v1'
_METADATA_HEADERS = {'Metadata-Flavor': 'Google'}
_NO_PROXY_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))
_DATAPROC_PROPERTIES_PATH = '/etc/google-dataproc/dataproc.properties'
_SPARK_DEFAULTS_PATH = '/etc/spark/conf/spark-defaults.conf'
_SAFE_APP_ID_RE = re.compile(r'^[A-Za-z0-9_.-]+$')
_YARN_APP_ID_RE = re.compile(r'^application_\d+_\d+$')


class CellMonitorExtension:
  """Manages the TCP socket, routing state, and IPython cell hooks."""

  def __init__(self, ipython):
    self.ipython = ipython
    self.port = None
    self.server = None
    self.socket_thread = None
    self.env_thread = None

    # State tracking
    self.run_id = None
    self.global_session_id = str(uuid.uuid4())
    self.active_widgets = {}
    self.job_to_run_id = {}
    self.sequence_counter = 0

    # Spark UI link state
    self._env_lock = threading.Lock()
    self.env_type = None  # 's8s' | 'dpgce' | 'unknown'
    self.project_id = ''
    self.region = ''
    self.dataproc_session_id = ''
    self.proxy_hostname = ''
    self.app_context_enabled = None
    self._live_conf_checked = False
    self.app_id = ''
    self.spark_ui_url = ''

  def start_server(self):
    """Starts TCP Server for listener metadata streaming."""
    self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.server.bind(('localhost', 0))
    self.port = self.server.getsockname()[1]
    self.server.listen(5)
    os.environ['SPARK_CELL_MONITOR_KERNEL_PORT'] = str(self.port)

    self.socket_thread = SocketThread(self.server, self)
    self.socket_thread.start()
    self._start_env_thread()

  def _start_env_thread(self):
    """Starts background daemon thread for Dataproc environment detection."""
    self.env_thread = threading.Thread(
        target=self._resolve_environment_bg, daemon=True
    )
    self.env_thread.start()

  @staticmethod
  def _fetch_metadata(path, timeout=0.5):
    """Fetches a GCE metadata value via link-local IP, bypassing any proxy."""
    req = urllib.request.Request(
        f'{_METADATA_IP_BASE}/{path}', headers=_METADATA_HEADERS
    )
    try:
      with _NO_PROXY_OPENER.open(req, timeout=timeout) as resp:
        if resp.status == 200:
          return resp.read().decode('utf-8').strip()
    except Exception:  # pylint: disable=broad-exception-caught
      pass
    return ''

  @staticmethod
  def _read_dataproc_proxy_hostname(path=None):
    """Reads and normalizes dataproc.proxy.public.hostname if CG is enabled."""
    path = path or _DATAPROC_PROPERTIES_PATH
    if not os.path.exists(path):
      return ''
    try:
      with open(path, encoding='utf-8') as f:
        for raw_line in f:
          line = raw_line.strip()
          if not line or line.startswith('#'):
            continue
          key, sep, val = line.partition('=')
          if sep == '=' and key.strip() == 'dataproc.proxy.public.hostname':
            host = val.strip().replace(r'\:', ':').rstrip('/')
            return host if host.startswith('https://') else ''
    except Exception:  # pylint: disable=broad-exception-caught
      pass
    return ''

  @staticmethod
  def _read_spark_defaults_app_context(path=None):
    """Checks spark.dataproc.appContext.enabled in spark-defaults.conf."""
    path = path or _SPARK_DEFAULTS_PATH
    if not os.path.exists(path):
      return True
    try:
      with open(path, encoding='utf-8') as f:
        for raw_line in f:
          line = raw_line.strip()
          if not line or line.startswith('#'):
            continue
          # Spark accepts "key value", "key=value" and "key = value".
          if '=' in line:
            key, _, value = line.partition('=')
          else:
            key, value = (line.split(None, 1) + [''])[:2]
          if key.strip() == 'spark.dataproc.appContext.enabled':
            return value.strip().lower() == 'true'
    except Exception:  # pylint: disable=broad-exception-caught
      pass
    return True

  @staticmethod
  def _is_valid_app_id(candidate):
    """Returns True if candidate is a real cluster/serverless Spark appId."""
    return (
        isinstance(candidate, str)
        and bool(candidate)
        and candidate != 'null'
        and not candidate.startswith('local-')
        and bool(_SAFE_APP_ID_RE.match(candidate))
    )

  def _resolve_environment_bg(self):
    """Detects Serverless vs. DPGCE in the background without stalling startup."""
    # Check Dataproc Serverless first (jupyter.sh also reads proxy hostname on s8s)
    session_id = self._fetch_metadata('instance/attributes/dataproc-session-id')
    if session_id:
      project_id = self._fetch_metadata('project/project-id')
      region = self._fetch_metadata('instance/attributes/dataproc-region')
      if not region:
        zone = self._fetch_metadata('instance/zone')
        if zone:
          zone_name = zone.rsplit('/', 1)[-1]
          region = '-'.join(zone_name.split('-')[:-1])
      default_app_ctx = self._read_spark_defaults_app_context()
      with self._env_lock:
        self.env_type = 's8s'
        self.dataproc_session_id = session_id
        self.project_id = project_id
        self.region = region
        if self.app_context_enabled is None:
          self.app_context_enabled = default_app_ctx
      self._refresh_spark_ui_url()
      return

    proxy_host = self._read_dataproc_proxy_hostname()
    with self._env_lock:
      if proxy_host:
        self.env_type = 'dpgce'
        self.proxy_hostname = proxy_host
      else:
        self.env_type = 'unknown'
    self._refresh_spark_ui_url()

  def _update_app_id(self, candidate_app_id):
    """Validates and records appId, then updates spark_ui_url if needed."""
    if not self._is_valid_app_id(candidate_app_id):
      return
    with self._env_lock:
      if self.app_id == candidate_app_id:
        return
      self.app_id = candidate_app_id
    self._refresh_spark_ui_url()

  def _refresh_spark_ui_url(self):
    """Computes the Spark UI URL and pushes updates to active widgets."""
    with self._env_lock:
      new_url = ''
      app_ctx = (
          True if self.app_context_enabled is None else self.app_context_enabled
      )
      if (
          self.env_type == 's8s'
          and app_ctx
          and self.project_id
          and self.region
          and self.dataproc_session_id
      ):
        enc_region = urllib.parse.quote(self.region, safe='')
        enc_session = urllib.parse.quote(self.dataproc_session_id, safe='')
        enc_project = urllib.parse.quote(self.project_id, safe='')
        if self.app_id:
          enc_app = urllib.parse.quote(self.app_id, safe='')
          new_url = (
              'https://console.cloud.google.com/dataproc/interactive/'
              f'{enc_region}/{enc_session}/sparkApplications/applications/'
              f'{enc_app}?project={enc_project}'
          )
        else:
          new_url = (
              'https://console.cloud.google.com/dataproc/interactive/'
              f'{enc_region}/{enc_session}/sparkApplications/applications'
              f'?project={enc_project}'
          )
      elif (
          self.env_type == 'dpgce'
          and self.proxy_hostname
          and self.app_id
          and _YARN_APP_ID_RE.match(self.app_id)
      ):
        enc_app = urllib.parse.quote(self.app_id, safe='')
        new_url = f'{self.proxy_hostname}/gateway/default/yarn/proxy/{enc_app}/'

      if new_url == self.spark_ui_url:
        return
      self.spark_ui_url = new_url
      # Snapshot under the lock so a widget registered concurrently by
      # pre_run_cell_hook either appears here or reads the new URL itself.
      widgets = list(self.active_widgets.values())

    for widget in widgets:
      widget.spark_ui_url = new_url

  # Threading note: send_to_frontend runs on the socket thread while the
  # IPython hooks and close_all_widgets run on the kernel thread. The shared
  # dicts are only ever touched through single, GIL-atomic operations
  # (get / pop(key, None) / next(iter(...), None) / item assignment), never
  # check-then-act, so a widget disappearing between two statements cannot
  # raise StopIteration or KeyError. Taking _env_lock here instead is not an
  # option: _handle_job_start -> _update_app_id already acquires it.

  def _any_active_widget(self):
    """Returns some live widget, or None; safe against concurrent removal."""
    return next(iter(self.active_widgets.values()), None)

  def _resolve_widget(self, target_id):
    """Finds the correct widget instance for a given session/job."""
    widget = self.active_widgets.get(target_id)
    if not widget:
      run_id = self.run_id
      if run_id:
        widget = self.active_widgets.get(run_id)
    if not widget:
      widget = self._any_active_widget()
    return widget

  def _get_target_run_id(self, spark_msg, msgtype):
    """Determines the correct run_id for the given message."""
    # jobGroup acts as the run_id when spark routing is used (e.g., Classic
    # PySpark). We fallback to self.run_id for Spark Connect or when
    # jobGroup isn't set.
    if msgtype == 'sparkJobStart':
      jg = spark_msg.get('jobGroup')
      return jg if jg and jg != 'null' and jg != '' else self.run_id
    
    if 'jobId' in spark_msg:
      return self.job_to_run_id.get(spark_msg['jobId'])
        
    return None

  def _handle_job_start(self, widget, spark_msg):
    # Map this new Spark job to the correct widget's run_id so future
    # stage/task events for this job can be routed to the same widget.
    self._update_app_id(spark_msg.get('appId', ''))
    self.job_to_run_id[spark_msg['jobId']] = widget.run_id
    widget.active_jobs_count += 1
    widget.append_event(spark_msg, self.sequence_counter)

  def _handle_job_end(self, widget, spark_msg):
    widget.append_event(spark_msg, self.sequence_counter)
    widget.active_jobs_count -= 1
    # If the Jupyter cell has finished executing AND all Spark jobs for
    # this cell have completed, it is safe to remove the widget from
    # active tracking.
    if widget.active_jobs_count <= 0 and getattr(widget, 'cell_finished', False):
      if self.active_widgets.pop(widget.run_id, None) is not None:
        self._finish_widget(widget)

    # Memory Cleanup: the job is finished, so we no longer need to track
    # its routing.
    self.job_to_run_id.pop(spark_msg.get('jobId'), None)

  def _handle_generic_event(self, widget, spark_msg):
    widget.append_event(spark_msg, self.sequence_counter)

  def close_all_widgets(self):
    """Removes every live monitor; used when monitoring is switched off."""
    with self._env_lock:
      widgets = list(self.active_widgets.values())
      self.active_widgets.clear()
      self.job_to_run_id.clear()
      self.run_id = None
    for widget in widgets:
      try:
        widget.cleanup(clear_history=True)
        widget.close()
      except Exception:  # pylint: disable=broad-exception-caught
        logger.debug('Error closing cell monitor widget', exc_info=True)

  def send_to_frontend(self, msg):
    """Routes a message to the appropriate frontend widget."""
    if not cell_monitoring.is_enabled():
      # Keep draining the listener socket, but nothing reaches the frontend.
      return
    # Strict Formatting Check: Must be a dict and must possess a msgtype.
    if not isinstance(msg, dict):
      logger.warning('Received malformed spark event: expected dictionary')
      return
      
    msgtype = msg.get('msgtype')
    if not msgtype:
      logger.warning("Received malformed spark event: missing 'msgtype' key")
      return

    try:
      spark_msg = msg
      self.sequence_counter += 1
      if msgtype == 'sparkApplicationStart':
        self._update_app_id(spark_msg.get('appId', ''))
      
      target_run_id = self._get_target_run_id(spark_msg, msgtype)
      widget = self._resolve_widget(target_run_id)
      
      if not widget:
        if msgtype == 'sparkJobEnd':
          logger.debug(
              'Could not find active widget for sparkJobEnd (jobId: %s)',
              spark_msg.get('jobId')
          )
          return
        # Fallback: if we can't map the event to a specific job, send it to
        # the most recent active widget.
        widget = self._any_active_widget()
        if widget is None:
          # Log when we completely drop an event because no widgets exist
          logger.debug('Dropped spark event (no active widgets found): %s', msgtype)
          return
        logger.debug('Message routed to fallback primary widget: %s', msgtype)

      # Route messages based on Job ID mappings
      if msgtype == 'sparkJobStart':
        self._handle_job_start(widget, spark_msg)
      elif msgtype == 'sparkJobEnd':
        self._handle_job_end(widget, spark_msg)
      else:
        self._handle_generic_event(widget, spark_msg)
          
      if isinstance(spark_msg, dict) and 'jobIds' in spark_msg:
        for jid in spark_msg['jobIds']:
          w = self._resolve_widget(self.job_to_run_id.get(jid))
          if w and w != widget:
            self._handle_generic_event(w, spark_msg)

    except Exception as e:  # pylint: disable=broad-exception-caught
      logger.warning('Error processing spark event', exc_info=True)

  def pre_run_cell_hook(self, *args, **kwargs):
    """Initializes tracking state and renders a new widget before a cell runs."""
    cell_info = args[0] if args else kwargs.get('info', None)
    if cell_info is not None:
      if getattr(cell_info, 'silent', False) or not getattr(cell_info, 'store_history', True):
        self.run_id = None
        return

    if not cell_monitoring.is_enabled():
      # Monitoring is off: run the cell untouched (no widget, no job group).
      self.run_id = None
      return

    self.sequence_counter = 0
    self.run_id = str(uuid.uuid4())

    try:
      from pyspark.sql import SparkSession  # pylint: disable=g-import-not-at-top

      session = SparkSession.getActiveSession() or SparkSession.getDefaultSession()

      if session and hasattr(session, "sparkContext"):
        if not self._live_conf_checked:
          try:
            live_flag = session.conf.get(
                'spark.dataproc.appContext.enabled', None
            )
            if live_flag is not None:
              with self._env_lock:
                self.app_context_enabled = (
                    str(live_flag).strip().lower() == 'true'
                )
              self._refresh_spark_ui_url()
            self._live_conf_checked = True
          except Exception:  # pylint: disable=broad-exception-caught
            pass
        if not self.app_id:
          self._update_app_id(
              getattr(session.sparkContext, 'applicationId', '')
          )
        session.sparkContext.setJobGroup(
            self.run_id, 'IPython Cell Execution', interruptOnCancel=True
        )
    except Exception as e:  # pylint: disable=broad-exception-caught
      logger.debug('Could not set job group in pre-run: %s', e)

    widget = ManagedSparkCellWidget(
        run_id=self.run_id, session_id=self.global_session_id
    )
    widget.cell_finished = False
    # Seed the URL and register the widget atomically with respect to
    # _refresh_spark_ui_url so no update can slip between the two steps.
    with self._env_lock:
      widget.spark_ui_url = self.spark_ui_url
      self.active_widgets[self.run_id] = widget
    IPython.display.display(widget)

  def post_run_cell_hook(self, result):
    """Marks the cell as finished and cleans up the widget if no jobs are active."""
    if result is not None and getattr(result, 'info', None) is not None:
      if getattr(result.info, 'silent', False) or not getattr(result.info, 'store_history', True):
        return

    widget = self.active_widgets.get(self.run_id)
    if widget is None:
      return
    widget.cell_finished = True
    if widget.active_jobs_count <= 0:
      # pop() rather than del: _handle_job_end on the socket thread may have
      # removed it already.
      if self.active_widgets.pop(self.run_id, None) is not None:
        self._finish_widget(widget)

  def _finish_widget(self, widget):
    """Closes out a widget whose cell has ended and whose jobs have all ended.

    This is the one moment the kernel knows the cell's final state is
    complete, so it pushes that state to the frontend before releasing the
    widget. Both the cell-end path and the job-end path funnel through here;
    whichever happens last performs the hand-off.
    """
    try:
      widget.send_final_state()
    except Exception:  # pylint: disable=broad-exception-caught
      logger.debug('Failed to send final state', exc_info=True)
    widget.cleanup()


class SocketThread(threading.Thread):
  """Background thread executing the TCP socket listener."""

  def __init__(self, server, extension_context):
    super(SocketThread, self).__init__()
    self.daemon = True
    self.server = server
    self.is_running = True
    self.extension_context = extension_context

  def run(self):
    """Socket listen loop."""
    while self.is_running:
      try:
        self.server.settimeout(5)
        client, _ = self.server.accept()
        reader = SocketReader(client, self.extension_context)
        reader.start()
      except socket.timeout:
        pass
      except Exception as e:  # pylint: disable=broad-exception-caught
        logger.debug('Socket accept interrupted or failed: %s', e)
        break


class SocketReader(threading.Thread):
  """Parser thread translating incoming raw listener packets into JSON."""

  def __init__(self, client, extension_context):
    super(SocketReader, self).__init__()
    self.daemon = True
    self.client = client
    self.extension_context = extension_context

  def run(self):
    """Buffered stream reader loop using custom ;EOD: delimiter."""
    buffer = b''
    while True:
      try:
        data = self.client.recv(8192)
        if not data:
          break
        buffer += data
        while b';EOD:' in buffer:
          line, buffer = buffer.split(b';EOD:', 1)
          if line:
            try:
              msg = json.loads(line.decode('utf-8'))
              self.extension_context.send_to_frontend(msg)
            except Exception as e:  # pylint: disable=broad-exception-caught
              logger.warning('Failed to parse JSON packet: %s', e)
      except Exception as e:  # pylint: disable=broad-exception-caught
        logger.debug('Socket read loop terminated: %s', e)
        break
    self.client.close()


def _patch_spark_context(extension_context):
  """Monkey-patches SparkContext.__init__ to automatically inject Job Groups."""
  _patch_spark_context.active_context = extension_context
  try:
    from pyspark import SparkContext  # pylint: disable=g-import-not-at-top

    if getattr(SparkContext.__init__, '_patched_by_cell_monitor', False):
      return

    orig_init = SparkContext.__init__

    def patched_init(self, *args, **kwargs):
      orig_init(self, *args, **kwargs)
      ctx = getattr(_patch_spark_context, 'active_context', None)
      if ctx and ctx.run_id:
        self.setJobGroup(
            ctx.run_id, 'ManagedSparkCellMonitoring cell tracking'
        )

    patched_init._patched_by_cell_monitor = True
    SparkContext.__init__ = patched_init
  except Exception as e:  # pylint: disable=broad-exception-caught
    logger.warning('Failed to monkey-patch SparkContext: %s', e)


def _register_cellmonitor_magic(ipython):
  """Registers `%cellmonitor on|off|status` as a user-facing toggle."""

  def cellmonitor(line=''):
    arg = (line or '').strip().lower()
    if arg in ('on', 'enable', 'enabled', 'true', '1'):
      cell_monitoring.set_enabled(True)
    elif arg in ('off', 'disable', 'disabled', 'false', '0'):
      cell_monitoring.set_enabled(False)
    elif arg not in ('', 'status'):
      print('Usage: %cellmonitor [on|off|status]')
      return
    state = 'on' if cell_monitoring.is_enabled() else 'off'
    print(f'Managed Spark cell monitoring is {state}')

  try:
    ipython.register_magic_function(
        cellmonitor, magic_kind='line', magic_name='cellmonitor'
    )
  except Exception:  # pylint: disable=broad-exception-caught
    logger.debug('Could not register %%cellmonitor magic', exc_info=True)


def load_ipython_extension(ipython):
  """Entrypoint, called when the extension is loaded."""
  if not ipykernel_imported:
    return
  if not isinstance(ipython, zmqshell.ZMQInteractiveShell):
    return

  # Encapsulate all state within a context object. The socket server is
  # started even when monitoring is currently off: the Spark driver reads the
  # port from the environment once at start-up, so it must always exist for a
  # later `set_enabled(True)` to work.
  extension_context = CellMonitorExtension(ipython)
  extension_context.start_server()
  cell_monitoring.add_disable_listener(extension_context.close_all_widgets)
  _register_cellmonitor_magic(ipython)

  if spark_imported:
    _patch_spark_context(extension_context)

  # Register the hooks
  ipython.events.register('pre_run_cell', extension_context.pre_run_cell_hook)
  ipython.events.register('post_run_cell', extension_context.post_run_cell_hook)
