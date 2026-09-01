# -*- coding: utf-8 -*-
# Copyright 2026 Google LLC
# Copyright 2017 CERN
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
import socket
import threading
import uuid

import IPython.display
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


class CellMonitorExtension:
  """Manages the TCP socket, routing state, and IPython cell hooks."""

  def __init__(self, ipython):
    self.ipython = ipython
    self.port = None
    self.server = None
    self.socket_thread = None

    # State tracking
    self.run_id = None
    self.global_session_id = str(uuid.uuid4())
    self.active_widgets = {}
    self.job_to_run_id = {}
    self.sequence_counter = 0

  def start_server(self):
    """Starts TCP Server for listener metadata streaming."""
    self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.server.bind(('localhost', 0))
    self.port = self.server.getsockname()[1]
    self.server.listen(5)
    os.environ['SPARK_CELL_MONITOR_KERNEL_PORT'] = str(self.port)

    self.socket_thread = SocketThread(self.server, self)
    self.socket_thread.start()

  def _resolve_widget(self, target_id):
    """Finds the correct widget instance for a given session/job."""
    widget = self.active_widgets.get(target_id)
    if not widget and self.run_id:
      widget = self.active_widgets.get(self.run_id)
    if not widget and self.active_widgets:
      widget = next(iter(self.active_widgets.values()))
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
      if widget.run_id in self.active_widgets:
        widget.cleanup()
        del self.active_widgets[widget.run_id]

    # Memory Cleanup: the job is finished, so we no longer need to track
    # its routing.
    job_id = spark_msg.get('jobId')
    if job_id in self.job_to_run_id:
      del self.job_to_run_id[job_id]

  def _handle_generic_event(self, widget, spark_msg):
    widget.append_event(spark_msg, self.sequence_counter)

  def send_to_frontend(self, msg):
    """Routes a message to the appropriate frontend widget."""
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
      
      target_run_id = self._get_target_run_id(spark_msg, msgtype)
      widget = self._resolve_widget(target_run_id)
      
      if not widget:
        if msgtype == 'sparkJobEnd':
          logger.debug('Could not find active widget for sparkJobEnd (jobId: %s)', spark_msg.get('jobId'))
          return
        elif self.active_widgets:
          # Fallback: if we can't map the event to a specific job, send it to
          # the most recent active widget.
          logger.debug('Message routed to fallback primary widget: %s', msgtype)
          widget = next(iter(self.active_widgets.values()))
        else:
          # Log when we completely drop an event because no widgets exist
          logger.debug('Dropped spark event (no active widgets found): %s', msgtype)
          return

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
    self.sequence_counter = 0
    self.run_id = str(uuid.uuid4())

    try:
      from pyspark.sql import SparkSession  # pylint: disable=g-import-not-at-top

      session = SparkSession.getActiveSession() or SparkSession.getDefaultSession()

      if session and hasattr(session, "sparkContext"):
        session.sparkContext.setJobGroup(
            self.run_id, 'IPython Cell Execution', interruptOnCancel=True
        )
    except Exception as e:  # pylint: disable=broad-exception-caught
      logger.debug('Could not set job group in pre-run: %s', e)

    widget = ManagedSparkCellWidget(
        run_id=self.run_id, session_id=self.global_session_id
    )
    widget.cell_finished = False
    self.active_widgets[self.run_id] = widget
    IPython.display.display(widget)

  def post_run_cell_hook(self, result):
    """Marks the cell as finished and cleans up the widget if no jobs are active."""
    if self.run_id in self.active_widgets:
      widget = self.active_widgets[self.run_id]
      widget.cell_finished = True
      if widget.active_jobs_count <= 0:
        widget.cleanup()
        del self.active_widgets[self.run_id]


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


def load_ipython_extension(ipython):
  """Entrypoint, called when the extension is loaded."""
  if not ipykernel_imported:
    return
  if not isinstance(ipython, zmqshell.ZMQInteractiveShell):
    return

  # Encapsulate all state within a context object
  extension_context = CellMonitorExtension(ipython)
  extension_context.start_server()

  if spark_imported:
    _patch_spark_context(extension_context)

  # Register the hooks
  ipython.events.register('pre_run_cell', extension_context.pre_run_cell_hook)
  ipython.events.register('post_run_cell', extension_context.post_run_cell_hook)
