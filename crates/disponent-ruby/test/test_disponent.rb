# Smoke test for the Ruby (Magnus) binding — the whole lifecycle over dry-run
# backends, mirroring the node bun test. Build first:
#   cd crates/disponent-ruby && bundle exec ruby extconf.rb && make
# Run:
#   bundle exec ruby -I. -Itest test/test_disponent.rb
require "minitest/autorun"
require "json"

# Dry-run backend flags must be in the process env before the engine opens;
# MRI's ENV[]= writes through to the native environment (unlike Bun).
ENV["DISPONENT_LOCAL_DRY_RUN"] = "1"
ENV["DISPONENT_EXE_DRY_RUN"] = "1"

require "disponent"

class TestDisponent < Minitest::Test
  # config_path (nil) + sink ("none" = memory-only).
  def open
    Disponent.new(nil, "none")
  end

  def test_whole_lifecycle
    d = open
    envs = d.environments
    assert_equal ["local", "exe-dev", "modal"], envs.map(&:slug)
    assert_equal "local", envs[0].kind # enum crosses as its wire string

    # per-env capabilities: one row per (env, capability); capability crosses as
    # its wire string. exe-dev advertises VM isolation; local does not.
    caps = d.capabilities
    assert(caps.any? { |c| c.env_slug == "local" && c.capability == "dispatch" })
    assert(caps.any? { |c| c.env_slug == "exe-dev" && c.capability == "isolation_vm" })
    refute(caps.any? { |c| c.env_slug == "local" && c.capability == "isolation_vm" })

    session = d.dispatch("say hi from ruby", "local")
    assert_equal "queued", session.state

    # Ruby omits the @manual wait(); poll session() until the dry-run
    # provisioner rides it to running (node/python use the blocking wait()).
    running = nil
    60.times do
      running = d.session(session.uid)
      break if running && running.state == "running"
      sleep 0.05
    end
    assert_equal "running", running.state
    assert_equal "dsp-#{session.uid}", JSON.parse(running.env_handle)["tmux"]

    # the event stream pages the timeline; payloads are JSON text
    events = d.events(session.uid)
    first = events.next
    assert_equal "log", first.kind
    assert_includes JSON.parse(first.payload)["payload"]["line"], "dispatch accepted"
    second = events.next
    assert_equal "state", second.kind

    # NB: our generated `send` shadows Ruby's Object#send on this class.
    d.send(session.uid, "how goes it?")

    cancelled = d.cancel(session.uid)
    assert_equal "cancelled", cancelled.state
    reaped = d.reap(session.uid)
    refute_nil reaped.reaped_at

    # filter crosses the optional-enum seam (state parsed from the wire string);
    # env is passed positionally (sessions uses scan_args, which can't skip it).
    done = d.sessions("local", "cancelled")
    assert_equal 1, done.length

    report = d.reconcile
    assert_equal 0, report.adopted
  end

  def test_driver_plan_drains
    d = open
    d.dispatch("row fodder", "local")

    plan = d.driver_plan("sqlite")
    sqls = []
    while (s = plan.next)
      sqls << s.sql
      assert_kind_of Array, JSON.parse(s.params)
    end
    assert sqls[0].start_with?("CREATE TABLE")
    assert(sqls.any? { |q| q.include?('INSERT INTO "dispatches"') })
  end

  def test_bad_inputs_fail_at_the_seam
    d = open
    # labels is the 12th (last) optional param; pass the 11 preceding as nil.
    err = assert_raises(RuntimeError) do
      # 2 required (brief, env) + 11 nil optionals, then labels (the 12th).
      d.dispatch("x", "local", nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, "not json")
    end
    assert_includes err.message, "labels"

    err2 = assert_raises(RuntimeError) { Disponent.new("/tmp/nope.toml", nil) }
    assert_includes err2.message, "configPath"
  end
end
