# frozen_string_literal: true

require "test_helper"

class SolidQueue::DatabaseCompatibilityTest < ActiveSupport::TestCase
  test "bulk enqueue and claim more than one thousand jobs" do
    jobs = 1_001.times.map { |i| AddToBufferJob.new(i) }

    assert_equal jobs.size, SolidQueue::Job.enqueue_all(jobs)
    claimed = SolidQueue::ReadyExecution.claim("*", jobs.size, 42)

    assert_equal jobs.map(&:provider_job_id).sort, claimed.map(&:job_id).sort
    assert_equal 0, SolidQueue::ReadyExecution.count
  end

  test "job arguments and queue names preserve unicode and long text" do
    # Leave room for serialized job metadata within MySQL's 65,535-byte TEXT limit.
    argument = "こんにちは, São Paulo! " * 1_500
    active_job = AddToBufferJob.new(argument).set(queue: "équipe_日本")
    job = SolidQueue::Job.enqueue(active_job).reload

    assert_equal argument, job.arguments.fetch("arguments").first
    assert_equal "équipe_日本", job.queue_name
    assert_equal [ job.id ], SolidQueue::ReadyExecution.claim("équipe_*", 1, 42).map(&:job_id)
  end

  test "queue prefixes treat SQL Server bracket patterns literally" do
    expected = SolidQueue::Job.enqueue(AddToBufferJob.new(1).set(queue: "[ab]_jobs"))
    SolidQueue::Job.enqueue(AddToBufferJob.new(2).set(queue: "a_jobs"))
    SolidQueue::Job.enqueue(AddToBufferJob.new(3).set(queue: "b_jobs"))

    assert_equal [ expected.id ], SolidQueue::ReadyExecution.claim("[ab]_*", 3, 42).map(&:job_id)
    assert_equal 2, SolidQueue::ReadyExecution.count
  end

  test "duplicate semaphore insertion leaves the surrounding transaction usable" do
    attributes = { key: "reserved-key", value: 1, expires_at: 1.minute.from_now }

    SolidQueue::Semaphore.transaction do
      assert SolidQueue::Semaphore.create_unique_by(attributes)
      assert_not SolidQueue::Semaphore.create_unique_by(attributes.merge(value: 10))
      assert_equal 1, SolidQueue::Semaphore.find_by!(key: attributes[:key]).value
      assert SolidQueue::Semaphore.create_unique_by(attributes.merge(key: "another-key"))
    end
  end

  test "creating executions in bulk skips existing jobs without replacing their attributes" do
    job = SolidQueue::Job.enqueue(AddToBufferJob.new(1))
    execution = job.ready_execution
    job.priority += 1

    assert_no_difference -> { SolidQueue::ReadyExecution.count } do
      SolidQueue::ReadyExecution.create_all_from_jobs([ job, job ])
    end

    assert_equal execution.priority, execution.reload.priority
  end

  test "recurring task upserts update the existing key and preserve its identity" do
    task = SolidQueue::RecurringTask.from_configuration("daily", class: "AddToBufferJob", schedule: "every day", args: [ "first" ])
    SolidQueue::RecurringTask.create_or_update_all([ task ])
    original = SolidQueue::RecurringTask.find_by!(key: "daily")
    task.schedule = "every hour"
    task.arguments = [ "updated" ]

    assert_no_difference -> { SolidQueue::RecurringTask.count } do
      SolidQueue::RecurringTask.create_or_update_all([ task ])
    end

    updated = SolidQueue::RecurringTask.find_by!(key: "daily")
    assert_equal original.id, updated.id
    assert_equal original.created_at, updated.created_at
    assert_equal "every hour", updated.schedule
    assert_equal [ "updated" ], updated.arguments
  end

  test "a conflict in a later claim insert rolls back all earlier inserts" do
    jobs = 1_001.times.map { |i| AddToBufferJob.new(i) }
    SolidQueue::Job.enqueue_all(jobs)
    SolidQueue::ClaimedExecution.insert_all!([ { job_id: jobs.last.provider_job_id, process_id: 43 } ])

    assert_no_difference [ -> { SolidQueue::ReadyExecution.count }, -> { SolidQueue::ClaimedExecution.count } ] do
      assert_raises(ActiveRecord::RecordNotUnique) { SolidQueue::ReadyExecution.claim("*", jobs.size, 42) }
    end
  end
end
