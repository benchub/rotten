require "rails_helper"

# The landing page reads home_stats and home_heatmap in one snapshot, so a
# refresh that commits between the two reads can't mix two refreshes.
RSpec.describe "Landing page stats", type: :request do
  before do
    User.delete_all
    ReportFixture.clear_home_stats!
    owner do |conn|
      conn.exec(<<~SQL)
        insert into rotten.home_stats (computed_at, as_of, events_last_hour, fingerprints, hosts_last_hour, hosts_last_week)
        values (now(), now() - interval '1 minute', 60, 1, 1, 1);
        insert into rotten.home_heatmap (hour, duration_bucket, fingerprints)
        values (date_trunc('hour', now() - interval '2 hours', 'UTC'), 0, 7);
      SQL
    end
    user = User.create!(email: "home.request@example.com", role: "viewer", active: true)
    post "/__test/sign_in", params: { user_id: user.id }
  end

  def owner
    conn = ReportFixture.connect
    yield conn
  ensure
    conn&.close
  end

  it "reads the stats and the heat map from the same refresh" do
    allow(HomeStats).to receive(:current).and_wrap_original do |original|
      stats = original.call
      # Another refresh commits after the stats row is read.
      owner do |conn|
        conn.exec(<<~SQL)
          delete from rotten.home_heatmap;
          insert into rotten.home_heatmap (hour, duration_bucket, fingerprints)
          values (date_trunc('hour', now() - interval '2 hours', 'UTC'), 0, 99);
        SQL
      end
      stats
    end

    get "/"

    expect(response).to have_http_status(:ok)
    titles = Nokogiri::HTML5(response.body).css("rect.heat-cell title").map(&:text)
    expect(titles).to contain_exactly(a_string_ending_with(": 7 fingerprints"))
  end
end
