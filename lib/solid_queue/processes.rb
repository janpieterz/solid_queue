# frozen_string_literal: true

module SolidQueue
  module Processes
    def self.without_signal_interruptions(&block)
      # TinyTDS 3 cancels queries when Ruby delivers a signal to the main
      # thread, including SIGCHLD during worker shutdown. Keep database work
      # off that thread while it continues to handle signals and wait for us.
      # https://github.com/rails-sqlserver/tiny_tds/issues/598
      if Thread.current == Thread.main && SolidQueue::Record.connection_db_config.adapter == "sqlserver"
        Thread.new(&block).value
      else
        yield
      end
    end
  end
end
