# frozen_string_literal: true

module SolidQueue
  class Execution
    module Dispatching
      extend ActiveSupport::Concern

      class_methods do
        def dispatch_jobs(job_ids)
          jobs = Job.where(id: job_ids)

          Job.dispatch_all(jobs).map(&:id).then do |dispatched_job_ids|
            delete_all_locked(where(job_id: dispatched_job_ids).non_blocking_read.pluck(:id))
          end
        end
      end
    end
  end
end
