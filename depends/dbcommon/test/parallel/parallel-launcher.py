#!/usr/bin/python
# Copyright (c) 2010 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

"""
This tool launches several shards of a gtest-based binary
in parallel on a local machine.

Example usage:

parallel_launcher.py path/to/base_unittests
"""

import optparse
import os
import subprocess
import sys
import threading
import time


def StreamCopyWindows(stream_from, stream_to):
  """Copies stream_from to stream_to."""

  while True:
    buf = stream_from.read(1024)
    if not buf:
      break
    stream_to.write(buf)
    stream_to.flush()

def StreamCopyPosix(stream_from, stream_to, child_exited):
  """
  Copies stream_from to stream_to, and exits if child_exited
  is signaled.
  """

  import fcntl

  # Put the source stream in a non-blocking mode, so we can check
  # child_exited when there is no data.
  fd = stream_from.fileno()
  fl = fcntl.fcntl(fd, fcntl.F_GETFL)
  fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

  while True:
    try:
      buf = os.read(fd, 1024)
    except OSError, e:
      if e.errno == 11 or e.errno == 35:
        if child_exited.isSet():
          break
        time.sleep(0.1)
        continue
      raise
    if not buf:
      break
    stream_to.write(buf)
    stream_to.flush()

class TestLauncher(object):
  def __init__(self, args, executable, num_shards, shard):
    self._args = args
    self._executable = executable
    self._num_shards = num_shards
    self._shard = shard
    self._test = None

  def launch(self):
    env = os.environ.copy()

    env['CHROME_LOG_FILE'] = 'chrome_log_%d' % self._shard

    if 'GTEST_TOTAL_SHARDS' in env:
      # Handle the requested sharding transparently.
      outer_shards = int(env['GTEST_TOTAL_SHARDS'])
      outer_index = int(env['GTEST_SHARD_INDEX'])

      env['GTEST_TOTAL_SHARDS'] = str(self._num_shards * outer_shards)

      # Calculate the right shard index to pass to the child. This is going
      # to be a shard of a shard.
      env['GTEST_SHARD_INDEX'] = str((self._num_shards * outer_index) +
                                     self._shard)
    else:
      env['GTEST_TOTAL_SHARDS'] = str(self._num_shards)
      env['GTEST_SHARD_INDEX'] = str(self._shard)

    args = self._args + ['--test-server-shard=' + str(self._shard)]

    self._test = subprocess.Popen(args=args,
                                  executable=self._executable,
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT,
                                  env=env)
  def wait(self):
    if subprocess.mswindows:
      stdout_thread = threading.Thread(
          target=StreamCopyWindows,
          args=[self._test.stdout, sys.stdout])
      stdout_thread.start()
      code = self._test.wait()
      stdout_thread.join()
      return code
    else:
      child_exited = threading.Event()
      stdout_thread = threading.Thread(
          target=StreamCopyPosix,
          args=[self._test.stdout, sys.stdout, child_exited])
      stdout_thread.start()
      code = self._test.wait()
      child_exited.set()
      stdout_thread.join()
      return code

def main(argv):
  parser = optparse.OptionParser()
  parser.add_option("--shards", type="int", dest="shards", default=16)

  # Make it possible to pass options to the launched process.
  # Options for parallel_launcher should be first, then the binary path,
  # and finally - optional arguments for the launched binary.
  parser.disable_interspersed_args()

  options, args = parser.parse_args(argv)

  if not args:
    print 'You must provide path to the test binary'
    return 1

  env = os.environ
  if bool('GTEST_TOTAL_SHARDS' in env) != bool('GTEST_SHARD_INDEX' in env):
    print 'Inconsistent environment. GTEST_TOTAL_SHARDS and GTEST_SHARD_INDEX'
    print 'should either be both defined, or both undefined.'
    return 1

  launchers = []

  for shard in range(options.shards):
    launcher = TestLauncher(args, args[0], options.shards, shard)
    launcher.launch()
    launchers.append(launcher)

  return_code = 0
  for launcher in launchers:
    if launcher.wait() != 0:
      return_code = 1

  return return_code

if __name__ == "__main__":
  sys.exit(main(sys.argv[1:]))
