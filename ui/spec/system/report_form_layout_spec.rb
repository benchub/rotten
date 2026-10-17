require "rails_helper"

# The Dataset fieldset's layout and help text: Time range lines up with the
# source fields, the Custom fields open under it, and chips are compact.
RSpec.describe "Report form layout", type: :system do
  before do
    User.delete_all
    ReportFixture.seed!
    user = User.create!(email: "layout-viewer@example.com", name: "Viewer", role: "viewer", active: true)
    visit "/__test/sign_in?user_id=#{user.id}"
  end

  def rect(selector)
    page.evaluate_script("(() => { const r = document.querySelector(#{selector.to_json}).getBoundingClientRect(); " \
                         "return { top: r.top, left: r.left, height: r.height, width: r.width }; })()")
  end

  it "shows the short help text for Match, Exclude and Ignore untagged context" do
    visit "/reports"
    expect(page).to have_css("#match-hint", exact_text: "Optional case-insensitive Postgres regex on the query text and context sources. Ignored in the time series.")
    expect(page).to have_css("#exclude-hint", exact_text: "Like Match, but the inverse.")
    expect(page).to have_css("#ignore_untagged-hint", exact_text: "Leave out queries without context. Ignored by the outliers report.")
  end

  it "puts Time range on Role's row, with the Custom fields under it" do
    visit "/reports"
    select "canvas", from: "Project"
    expect(page).to have_css(".source-chip", text: "canvas")
    expect(rect("label[for=range]")["top"]).to eq(rect("label[for=role]")["top"])

    select "Custom", from: "Time range"
    expect(page).to have_field("From (UTC)")
    range = rect("#range")
    custom = rect(".custom-range")
    expect(custom["left"]).to be >= range["left"]
    expect(custom["top"]).to be >= range["top"] + range["height"]
    expect(rect("label[for=range]")["top"]).to eq(rect("label[for=role]")["top"])
  end

  it "draws compact chips with the remove button in the upper-right corner" do
    visit "/reports"
    select "canvas", from: "Project"
    fill_in "Match", with: "foo"
    click_button "Add match pattern"
    [".source-chip", ".pattern-chip"].each do |chip|
      box = rect(chip)
      button = rect("#{chip} .source-chip-remove")
      expect(box["height"]).to be < 30
      expect(button["height"]).to be <= 16
      expect(button["top"] - box["top"]).to be < 4
      expect((box["left"] + box["width"]) - (button["left"] + button["width"])).to be < 4
    end
    color = page.evaluate_script("getComputedStyle(document.querySelector('.source-chip-remove')).color")
    expect(color).to eq("rgb(180, 35, 24)")
    find(".pattern-chip .source-chip-remove").send_keys(:enter)
    expect(page).to have_no_css(".pattern-chip code")
  end
end
