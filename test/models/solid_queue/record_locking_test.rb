# frozen_string_literal: true

require "test_helper"

class SolidQueue::RecordLockingTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  setup do
    skip "SQLite does not support row-level locking" if SolidQueue::Record.connection.adapter_name == "SQLite"
    @previous_skip_locked = SolidQueue.use_skip_locked
    SolidQueue.use_skip_locked = true
  end

  teardown do
    SolidQueue.use_skip_locked = @previous_skip_locked unless @previous_skip_locked.nil?
  end

  test "workers skip locked ready executions and claim them after the lock is released" do
    jobs = 3.times.map { |i| SolidQueue::Job.enqueue(AddToBufferJob.new(i)) }
    candidates = SolidQueue::ReadyExecution.ordered.limit(1).select(:id, :job_id)

    with_locked_relation(candidates) do
      claimed = SolidQueue::ReadyExecution.claim("*", 3, 42)
      assert_equal jobs.drop(1).map(&:id), claimed.map(&:job_id).sort
    end

    assert_equal [ jobs.first.id ], SolidQueue::ReadyExecution.claim("*", 3, 42).map(&:job_id)
    assert_equal 0, SolidQueue::ReadyExecution.count
  end

  test "prefix queue discovery skips an uncommitted ready execution deletion" do
    assert_queue_discovery_skips_uncommitted_deletion("discovery_*")
  end

  test "wildcard queue discovery with paused queues skips an uncommitted ready execution deletion" do
    pause = SolidQueue::Pause.create!(queue_name: "discovery_paused")
    assert_queue_discovery_skips_uncommitted_deletion("*")
  ensure
    pause&.destroy!
  end

  test "disabling skip locked makes prefix queue discovery wait" do
    assert_queue_discovery_waits_for_uncommitted_deletion("discovery_*")
  end

  test "disabling skip locked makes wildcard queue discovery with paused queues wait" do
    pause = SolidQueue::Pause.create!(queue_name: "discovery_paused")
    assert_queue_discovery_waits_for_uncommitted_deletion("*")
  ensure
    pause&.destroy!
  end

  test "dispatchers skip locked scheduled executions without losing them" do
    jobs = 3.times.map { |i| SolidQueue::Job.enqueue(AddToBufferJob.new(i), scheduled_at: 1.minute.from_now) }
    travel_to 2.minutes.from_now

    with_locked_relation(SolidQueue::ScheduledExecution.next_batch(1).select(:job_id)) do
      assert_equal 2, SolidQueue::ScheduledExecution.dispatch_next_batch(3)
    end

    assert_equal 1, SolidQueue::ScheduledExecution.dispatch_next_batch(3)
    assert_equal jobs.map(&:id), SolidQueue::ReadyExecution.order(:job_id).pluck(:job_id)
  end

  test "blocked execution release skips locked rows" do
    result = JobResult.create!
    3.times { NonOverlappingUpdateResultJob.perform_later(result) }
    ready_job = SolidQueue::Job.order(:id).first
    blocked = SolidQueue::BlockedExecution.ordered.to_a
    SolidQueue::Semaphore.signal(ready_job)

    candidates = SolidQueue::BlockedExecution.where(concurrency_key: ready_job.concurrency_key).ordered.limit(1)
      .use_index(:index_solid_queue_blocked_executions_for_release)
    with_locked_relation(candidates) do
      assert SolidQueue::BlockedExecution.release_one(ready_job.concurrency_key)
    end

    assert SolidQueue::BlockedExecution.exists?(blocked.first.id)
    assert_not SolidQueue::BlockedExecution.exists?(blocked.last.id)
    assert SolidQueue::Job.find(blocked.last.job_id).ready?
  end

  test "concurrent workers with overlapping queue selectors claim each job exactly once" do
    jobs = 24.times.map { |i| AddToBufferJob.new(i) }
    SolidQueue::Job.enqueue_all(jobs)
    start = Concurrent::Event.new
    workers = 4.times.map do |i|
      Thread.new do
        SolidQueue::Record.connection_pool.with_connection do
          start.wait(5)
          claimed_ids = []
          loop do
            claimed = SolidQueue::ReadyExecution.claim(i.even? ? "*" : "background", 3, i + 1)
            break if claimed.empty?
            claimed_ids.concat(claimed.map(&:job_id))
          end
          claimed_ids
        end
      end
    end
    start.set

    claimed_ids = workers.flat_map(&:value)
    assert_equal jobs.map(&:provider_job_id).sort, claimed_ids.sort
    assert_equal 24, SolidQueue::ClaimedExecution.count
    assert_equal 0, SolidQueue::ReadyExecution.count
  ensure
    workers&.each { |worker| worker.kill.join if worker.alive? }
  end

  test "disabling skip locked waits for the locked row" do
    SolidQueue::Job.enqueue(AddToBufferJob.new(1))
    candidates = SolidQueue::ReadyExecution.ordered.limit(1).select(:id, :job_id)
    started = Concurrent::Event.new
    completed = Concurrent::Event.new
    claimant = nil

    with_locked_relation(candidates) do
      SolidQueue.use_skip_locked = false
      claimant = Thread.new do
        SolidQueue::Record.connection_pool.with_connection do
          started.set
          SolidQueue::ReadyExecution.claim("*", 1, 42).tap { completed.set }
        end
      end
      assert started.wait(5), "Claimant did not start"
      assert_not completed.wait(0.2), "Claimant should wait until the row lock is released"
    end

    assert_equal 1, claimant.value.size
    assert completed.set?
  ensure
    claimant.kill.join if claimant&.alive?
  end

  private
    def assert_queue_discovery_waits_for_uncommitted_deletion(selector)
      job = SolidQueue::Job.enqueue(AddToBufferJob.new(1).set(queue: "discovery_locked"))
      started = Concurrent::Event.new
      completed = Concurrent::Event.new
      claimant = nil

      with_locked_relation(SolidQueue::ReadyExecution.where(job_id: job.id), delete: true) do
        SolidQueue.use_skip_locked = false
        claimant = Thread.new do
          SolidQueue::Record.connection_pool.with_connection do
            started.set
            SolidQueue::ReadyExecution.claim(selector, 1, 42).tap { completed.set }
          end
        end

        assert started.wait(5), "Claimant did not start"
        assert_not completed.wait(0.2), "Queue discovery should wait until the deletion commits"
      end

      assert_empty claimant.value
      assert completed.set?
    ensure
      claimant.kill.join if claimant&.alive?
    end

    def assert_queue_discovery_skips_uncommitted_deletion(selector)
      jobs = %w[ discovery_a discovery_b discovery_c ].map do |queue|
        SolidQueue::Job.enqueue(AddToBufferJob.new(queue).set(queue: queue))
      end

      with_locked_relation(SolidQueue::ReadyExecution.where(job_id: jobs.first.id), delete: true) do
        assert_equal [ jobs.second.id ], SolidQueue::ReadyExecution.claim("discovery_b", 1, 42).map(&:job_id)
        with_sqlserver_lock_timeout do
          assert_equal [ jobs.third.id ], SolidQueue::ReadyExecution.claim(selector, 1, 43).map(&:job_id)
        end
      end
    end

    def with_sqlserver_lock_timeout
      SolidQueue::Record.connection_pool.with_connection do |connection|
        if connection.adapter_name == "SQLServer"
          previous_timeout = connection.select_value("SELECT @@LOCK_TIMEOUT")
          begin
            connection.execute("SET LOCK_TIMEOUT 1000")
            yield
          ensure
            connection.execute("SET LOCK_TIMEOUT #{previous_timeout}")
          end
        else
          yield
        end
      end
    end

    def with_locked_relation(relation, delete: false)
      locked = Concurrent::Event.new
      release = Concurrent::Event.new
      locker = Thread.new do
        SolidQueue::Record.connection_pool.with_connection do
          SolidQueue::Record.transaction do
            relation.non_blocking_lock.load
            relation.delete_all if delete
            locked.set
            raise "Timed out waiting to release row lock" unless release.wait(10)
          end
        end
      end

      assert locked.wait(5), "Failed to acquire row lock"
      yield
    ensure
      release&.set
      locker&.value
    end
end
