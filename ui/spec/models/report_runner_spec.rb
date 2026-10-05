require "rails_helper"

RSpec.describe ReportRunner do
  def setting(name)
    ApplicationRecord.connection.select_value("select current_setting('#{name}')")
  end

  it "runs SQL with bound parameters and casts the values" do
    result = described_class.new(timeout_ms: 5_000).run("select $1::text as a, $2::bigint as b, $3::numeric as c",
                                                         ["x'; --", 42, "1.5"])

    expect(result.columns).to eq(%w[a b c])
    expect(result.rows).to eq([["x'; --", 42, BigDecimal("1.5")]])
  end

  it "applies the statement timeout to the report query" do
    result = described_class.new(timeout_ms: 1234).run("select current_setting('statement_timeout') as t", [])

    expect(result.rows).to eq([["1234ms"]])
  end

  it "doesn't leak the timeout to the connection afterwards" do
    before = setting("statement_timeout")
    described_class.new(timeout_ms: 1234).run("select 1", [])

    expect(setting("statement_timeout")).to eq(before)
  end

  it "runs the report query with JIT off" do
    result = described_class.new(timeout_ms: 5_000).run("select current_setting('jit') as j", [])

    expect(result.rows).to eq([["off"]])
  end

  it "doesn't leak JIT off to the connection afterwards" do
    expect(setting("jit")).to eq("on")
    described_class.new(timeout_ms: 5_000).run("select 1", [])

    expect(setting("jit")).to eq("on")
  end

  it "doesn't leak JIT off when the report fails" do
    expect { described_class.new(timeout_ms: 5_000).run("select 1/0", []) }
      .to raise_error(ActiveRecord::StatementInvalid)

    expect(setting("jit")).to eq("on")
  end

  it "runs in a read-only transaction" do
    expect { described_class.new(timeout_ms: 5_000).run("create temporary table report_runner_probe (x int)", []) }
      .to raise_error(ActiveRecord::StatementInvalid, /read-only/)
  end

  it "raises QueryCanceled when the query runs past the timeout, and the connection still works" do
    expect { described_class.new(timeout_ms: 50).run("select pg_sleep(2)", []) }
      .to raise_error(ActiveRecord::QueryCanceled)

    expect(ApplicationRecord.connection.select_value("select 1")).to eq(1)
    expect(setting("statement_timeout")).not_to eq("50ms")
  end

  it "shares one time budget across its queries" do
    runner = described_class.new(timeout_ms: 1_000)
    runner.run("select pg_sleep(0.6)", [])
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    expect { runner.run("select pg_sleep(0.7)", []) }.to raise_error(ActiveRecord::QueryCanceled)
    expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 0.65
  end

  it "gives the next query only what's left of the budget" do
    runner = described_class.new(timeout_ms: 5_000)
    runner.run("select pg_sleep(0.5)", [])

    sql = "select setting::integer from pg_settings where name = 'statement_timeout'"
    left = runner.run(sql, []).rows.first.first
    expect(left).to be_between(1_000, 4_600)
  end

  it "raises QueryCanceled without querying once the budget is spent" do
    runner = described_class.new(timeout_ms: 50)
    expect { runner.run("select pg_sleep(1)", []) }.to raise_error(ActiveRecord::QueryCanceled)

    expect(ApplicationRecord).not_to receive(:with_connection)
    expect { runner.run("select 1", []) }.to raise_error(ActiveRecord::QueryCanceled)
  end

  it "reports how much of the budget is left, starting the clock on first ask" do
    runner = described_class.new(timeout_ms: 5_000)
    expect(runner.remaining_ms).to be_between(4_900, 5_000)

    runner.run("select pg_sleep(0.5)", [])
    expect(runner.remaining_ms).to be_between(1_000, 4_550)
  end

  it "uses the configured timeout by default" do
    original = Rails.configuration.x.report_timeout_ms
    Rails.configuration.x.report_timeout_ms = 4321

    expect(described_class.new.run("select current_setting('statement_timeout') as t", []).rows).to eq([["4321ms"]])
  ensure
    Rails.configuration.x.report_timeout_ms = original
  end
end
