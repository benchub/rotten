require "rails_helper"

# Report tables cut long query text to one line. A keyboard user must be able
# to open the cell and read the whole query, not just hover for a title.
RSpec.describe "Query text in report tables", type: :system do
  TAIL = "and tail_marker_column = 'the very end of the query'".freeze
  LONG_SQL = "select * from users where id = $1 and #{(1..5).map { |i| "padding_column_#{i} = $#{i + 1}" }.join(' and ')} #{TAIL}".freeze

  before do
    User.delete_all
    ReportFixture.seed!
    conn = ReportFixture.connect
    begin
      conn.exec_params("update rotten.fingerprints set normalized = $1 where normalized = $2",
                       [LONG_SQL, "select * from users where id = $1"])
    ensure
      conn.close
    end
    user = User.create!(email: "query-text@example.com", name: "Viewer", role: "viewer", active: true)
    visit "/__test/sign_in?user_id=#{user.id}"
  end

  # Where the end of the query sits relative to the box that clips it.
  def tail_geometry
    page.evaluate_script(<<~JS)
      (function() {
        var cells = Array.from(document.querySelectorAll("td[data-column='example'] .query-text"));
        var line = cells.find(function(el) { return el.textContent.indexOf("tail_marker_column") >= 0; });
        var text = line.firstChild;
        var range = document.createRange();
        range.setStart(text, text.length - 5);
        range.setEnd(text, text.length);
        var tail = range.getBoundingClientRect();
        var box = line.getBoundingClientRect();
        return { inside: tail.width > 0 && tail.right <= box.right + 1 && tail.bottom <= box.bottom + 1,
                 clipped: line.scrollWidth > line.clientWidth };
      })()
    JS
  end

  it "opens a truncated query from the keyboard to show all of it" do
    visit "/reports?report=top_by_calls&project=canvas&environment=production&cluster=13&range=3h"
    cell = find("td[data-column='example']", text: "tail_marker_column")

    expect(cell.text).to eq(LONG_SQL)
    expect(tail_geometry).to eq("inside" => false, "clipped" => true)

    summary = cell.find("details summary")
    summary.send_keys(:enter)
    expect(page.active_element).to eq(summary)
    expect(cell).to have_css("details[open]")
    expect(tail_geometry).to eq("inside" => true, "clipped" => false)
    expect(cell.text).to eq(LONG_SQL)

    summary.send_keys(:space)
    expect(cell).to have_no_css("details[open]")
    expect(tail_geometry).to eq("inside" => false, "clipped" => true)
  end
end
