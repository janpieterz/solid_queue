# frozen_string_literal: true

module SolidQueue
  class Execution < Record
    class UndiscardableError < StandardError; end

    include JobAttributes

    self.abstract_class = true

    scope :ordered, -> { order(priority: :asc, job_id: :asc) }

    belongs_to :job

    class << self
      def type
        model_name.element.sub("_execution", "").to_sym
      end

      def create_all_from_jobs(jobs)
        rows = execution_data_from_jobs(jobs)
        if connection_pool.with_connection(&:supports_insert_on_duplicate_skip?)
          insert_all rows
        else
          # Older SQL Server adapters don't implement insert_all's duplicate skipping.
          transaction do
            rows.each do |attributes|
              create_or_find_by!(job_id: attributes.fetch(:job_id)) do |execution|
                execution.assign_attributes(attributes)
              end
            end
          end
        end
      end

      def execution_data_from_jobs(jobs)
        jobs.collect do |job|
          attributes_from_job(job).merge(job_id: job.id)
        end
      end

      # These rows have already been locked by this transaction. SQL Server can
      # scan other rows while deleting them, so skip locks held by other pollers.
      def delete_all_locked(ids)
        if sqlserver?
          statement = Arel::DeleteManager.new
            .from(Arel.sql("#{quoted_table_name} WITH (ROWLOCK, READPAST, READCOMMITTEDLOCK)"))
            .where(arel_table[:id].in(ids))
          connection_pool.with_connection { |connection| connection.delete(statement) }
        else
          where(id: ids).delete_all
        end
      end

      def discard_all_in_batches(batch_size: 500)
        # Newly unblocked executions must survive later discard batches.
        last_id = maximum(:id) || 0
        pending = where(id: ..last_id).count
        discarded = 0

        SolidQueue.instrument(:discard_all, batch_size: batch_size, status: type, batches: 0, size: 0) do |payload|
          loop do
            transaction do
              job_ids = where(id: ..last_id).limit(batch_size).order(:job_id).lock.pluck(:job_id)
              discarded = discard_jobs job_ids

              where(job_id: job_ids).delete_all
              pending -= discarded

              payload[:size] += discarded
              payload[:batches] += 1
            end

            break if pending <= 0 || discarded == 0
          end
        end
      end

      def discard_all_from_jobs(jobs)
        SolidQueue.instrument(:discard_all, jobs_size: jobs.size, status: type) do |payload|
          transaction do
            job_ids = lock_all_from_jobs(jobs)

            payload[:size] = discard_jobs job_ids
            where(job_id: job_ids).delete_all
          end
        end
      end

      private
        def lock_all_from_jobs(jobs)
          where(job_id: jobs.map(&:id)).order(:job_id).lock.pluck(:job_id)
        end

        def discard_jobs(job_ids)
          Job.where(id: job_ids).delete_all
        end
    end

    def type
      self.class.type
    end

    def discard
      SolidQueue.instrument(:discard, job_id: job_id, status: type) do
        with_lock do
          job.destroy
          destroy
        end
      end
    end
  end
end
