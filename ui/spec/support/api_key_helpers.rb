require "pg"

# rotten_ui can't delete pass keys or audit rows, or read secret_hash, so
# these helpers use the rotten_owner connection from
# ROTTEN_UI_TEST_SEED_DATABASE_URL (see ReportFixture.connect).
module ApiKeyHelpers
  def self.with_owner
    conn = ReportFixture.connect
    yield conn
  ensure
    conn&.close
  end

  def self.clear!
    with_owner do |conn|
      conn.exec(<<~SQL)
        delete from rotten.ingested_batches;
        delete from rotten.ui_audit_log;
        delete from rotten.api_keys;
      SQL
    end
  end

  # Every column of the key, secret_hash included.
  def owner_api_key(id)
    ApiKeyHelpers.with_owner do |conn|
      conn.exec_params("select * from rotten.api_keys where id = $1", [id]).first
    end
  end

  def owner_api_key_count
    ApiKeyHelpers.with_owner { |conn| conn.exec("select count(*) from rotten.api_keys").getvalue(0, 0).to_i }
  end

  def owner_audit_rows
    ApiKeyHelpers.with_owner do |conn|
      conn.exec("select actor_user_id, actor_email, action, target_type, target_id, details, at from rotten.ui_audit_log order by id").to_a
    end
  end

  # A key made the way the CLI makes one, as rotten_owner.
  def owner_create_api_key(name:, fqdn: "db.example.test", created_by: "cli")
    ApiKeyHelpers.with_owner do |conn|
      conn.exec_params("insert into rotten.api_keys (name, secret_hash, fqdn, created_by) values ($1, $2, $3, $4) returning id",
                       [name, SecureRandom.hex(32), fqdn, created_by]).getvalue(0, 0).to_i
    end
  end
end

RSpec.configure do |config|
  config.include ApiKeyHelpers, :api_keys
  config.before(:each, :api_keys) { ApiKeyHelpers.clear! }
end
