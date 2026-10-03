require "rails_helper"

RSpec.describe "Health check", type: :request do
  around do |example|
    original_config = ActiveRecord::Base.connection_db_config
    example.run
  ensure
    ActiveRecord::Base.establish_connection(original_config)
  end

  it "returns 200 when the database is reachable" do
    get "/up"

    expect(response).to have_http_status(:ok)
  end

  it "returns 503 when the database is down" do
    ActiveRecord::Base.establish_connection(
      adapter: "postgresql",
      database: "rotten",
      host: "127.0.0.1",
      port: 1,
      username: "rotten_ui",
      password: "rotten_ui",
      connect_timeout: 1
    )

    get "/up"

    expect(response).to have_http_status(:service_unavailable)
  end

  it "returns 503 when the database query times out" do
    connection = ActiveRecord::Base.connection
    allow(connection).to receive(:select_value).and_raise(Timeout::Error)
    allow(ActiveRecord::Base.connection_pool).to receive(:disconnect!).and_raise(
      ActiveRecord::ExclusiveConnectionTimeoutError
    )

    get "/up"

    expect(response).to have_http_status(:service_unavailable)
  end
end
