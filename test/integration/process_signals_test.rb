# frozen_string_literal: true

require "test_helper"

class ProcessSignalsTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  test "child exit signals do not cancel SQL Server process queries" do
    skip unless SolidQueue::Record.connection.adapter_name == "SQLServer"

    reader, writer = IO.pipe
    # Exercise a real main thread even when the test runner is on a background
    # thread, so the test still fails if the production protection is removed.
    pid = fork do
      reader.close
      exiting_child = fork do
        sleep 0.2
        exit!(0)
      end

      result = SolidQueue::Processes.without_signal_interruptions do
        SolidQueue::Record.connection_pool.with_connection do |connection|
          connection.select_value("WAITFOR DELAY '00:00:01'; SELECT 123")
        end
      end

      Marshal.dump([ Thread.current == Thread.main, result ], writer)
      writer.close
      Process.wait(exiting_child)
      exit!(0)
    end
    writer.close

    on_main_thread, result = Marshal.load(reader)
    assert on_main_thread

    assert_equal 123, result
  ensure
    reader&.close unless reader&.closed?
    writer&.close unless writer&.closed?
    Process.wait(pid) if pid
  end
end
