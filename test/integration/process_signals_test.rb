# frozen_string_literal: true

require "test_helper"

class ProcessSignalsTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  setup do
    skip unless SolidQueue::Record.connection.adapter_name == "SQLServer"
  end

  test "child exit signals do not cancel SQL Server process queries" do
    result = with_child_exiting_during_query do
      SolidQueue::Processes.without_signal_interruptions { delayed_query }
    end

    assert_equal 123, result
  end

  test "supervisor configuration queries are protected from child exit signals" do
    result = with_child_exiting_during_query do
      configuration = SolidQueue::Configuration.new(mode: :async, standalone: false,
        workers: [ { queues: "background" } ], dispatchers: [], skip_recurring: true)
      validation_result = nil
      configuration.define_singleton_method(:valid?) do
        validation_result = SolidQueue::Record.connection.select_value("WAITFOR DELAY '00:00:01'; SELECT 123")
        super()
      end
      SolidQueue::Configuration.stubs(:new).returns(configuration)

      supervisor = SolidQueue::Supervisor.start
      supervisor.stop
      validation_result
    end

    assert_equal 123, result
  end

  private
    def delayed_query
      SolidQueue::Record.connection_pool.with_connection do |connection|
        connection.select_value("WAITFOR DELAY '00:00:01'; SELECT 123")
      end
    end

    def with_child_exiting_during_query
      reader, writer = IO.pipe
      # Exercise a real main thread even when the test runner is on a background
      # thread, so the test still fails if the production protection is removed.
      pid = fork do
        reader.close
        exiting_child = fork do
          sleep 0.2
          exit!(0)
        end

        result = yield
        Marshal.dump([ Thread.current == Thread.main, result ], writer)
        writer.close
        Process.wait(exiting_child)
        exit!(0)
      end
      writer.close

      on_main_thread, result = Marshal.load(reader)
      assert on_main_thread
      result
    ensure
      reader&.close unless reader&.closed?
      writer&.close unless writer&.closed?
      Process.wait(pid) if pid
    end
end
