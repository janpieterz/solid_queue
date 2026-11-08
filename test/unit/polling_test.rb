# frozen_string_literal: true

require "test_helper"

class PollingTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  setup do
    skip unless SolidQueue::Record.connection.adapter_name == "SQLServer"
  end

  test "wait while checks its deadline without interrupting a database query" do
    result = nil

    assert_raises Timeout::Error do
      wait_while_with_timeout!(0.05) do
        result = SolidQueue::Record.connection.select_value("WAITFOR DELAY '00:00:00.150'; SELECT 123")
        true
      end
    end

    assert_equal 123, result
    assert_equal 456, SolidQueue::Record.connection.select_value("SELECT 456")
  end

  test "wait for checks its deadline without interrupting a database query" do
    result = nil

    assert_raises Timeout::Error do
      wait_for(timeout: 0.05) do
        result = SolidQueue::Record.connection.select_value("WAITFOR DELAY '00:00:00.150'; SELECT 123")
        false
      end
    end

    assert_equal 123, result
    assert_equal 456, SolidQueue::Record.connection.select_value("SELECT 456")
  end
end
