# frozen_string_literal: true

require "test_helper"

class ProcessesTest < ActiveSupport::TestCase
  test "other adapters run on the calling thread" do
    Thread.stubs(:main).returns(Thread.current)
    %w[ mysql2 postgresql sqlite3 ].each do |adapter|
      SolidQueue::Record.stubs(:connection_db_config).returns(stub(adapter: adapter))
      assert_equal Thread.current, SolidQueue::Processes.without_signal_interruptions { Thread.current }
    end
  end

  test "SQL Server work already running on a background thread stays there" do
    SolidQueue::Record.stubs(:connection_db_config).returns(stub(adapter: "sqlserver"))
    thread = Thread.new { SolidQueue::Processes.without_signal_interruptions { Thread.current } }
    assert_equal thread, thread.value
  end

  test "SQL Server work on the main thread returns its result and propagates exceptions" do
    Thread.stubs(:main).returns(Thread.current)
    SolidQueue::Record.stubs(:connection_db_config).returns(stub(adapter: "sqlserver"))
    assert_not_equal Thread.main, SolidQueue::Processes.without_signal_interruptions { Thread.current }
    assert_equal 42, SolidQueue::Processes.without_signal_interruptions { 42 }
    assert_raises(ExpectedTestError) do
      SolidQueue::Processes.without_signal_interruptions do
        Thread.current.report_on_exception = false
        raise ExpectedTestError
      end
    end
  end
end
