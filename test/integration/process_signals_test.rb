# frozen_string_literal: true

require "test_helper"

class ProcessSignalsTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  test "child exit signals do not cancel SQL Server process queries" do
    skip unless SolidQueue::Record.connection.adapter_name == "SQLServer"

    pid = fork do
      sleep 0.2
      exit!(0)
    end

    result = SolidQueue::Processes.without_signal_interruptions do
      SolidQueue::Record.connection_pool.with_connection do |connection|
        connection.select_value("WAITFOR DELAY '00:00:01'; SELECT 123")
      end
    end

    assert_equal 123, result
  ensure
    Process.wait(pid) if pid
  end
end
