require "rails_helper"

RSpec.describe ReportsHelper, type: :helper do
  def cell(column_key, type, value)
    column = Report::Column.new(key: column_key, label: column_key.capitalize, type: type)
    # As a view renders it: a plain String is escaped, a SafeBuffer is not.
    Nokogiri::HTML5.fragment(ERB::Util.html_escape(helper.report_cell(nil, column, value)))
  end

  describe "#report_cell for query text" do
    let(:sql) { "select * from users where id = $1 and name = $2 <script>alert(1)</script> " + ("x" * 300) }

    it "puts the whole query, once, in a disclosure that opens to show it" do
      fragment = cell("example", :text, sql)
      span = fragment.at_css("details.query-disclosure > summary > span.query-text")

      expect(span).not_to be_nil
      expect(span.text).to eq(sql)
      expect(span["title"]).to eq(sql)
      expect(fragment.text).to eq(sql)
    end

    it "escapes the query" do
      fragment = cell("example", :text, sql)

      expect(fragment.css("script")).to be_empty
    end

    it "leaves other text columns plain" do
      expect(cell("role", :text, "primary").to_html).to eq("primary")
    end

    it "renders nothing for a missing query" do
      expect(cell("example", :text, nil).to_html).to eq("")
    end
  end
end
