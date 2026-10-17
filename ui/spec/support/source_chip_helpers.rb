# The report form's source fields keep their chosen values as chips under
# each dropdown (source_picker_controller.js).
module SourceChipHelpers
  # The chips of the field labeled field, as [value, stale?].
  def source_chip_states(field)
    find_field(field).find(:xpath, "following-sibling::ul[contains(@class, 'source-chips')]", visible: :all)
                     .all("li.source-chip").map { |chip| [chip.find("span", match: :first).text, chip[:class].include?("stale")] }
  end

  # The chips' values.
  def source_chips(field) = source_chip_states(field).map(&:first)

  # Makes value the field's only chip.
  def pick_only(value, from:)
    select "All #{from.downcase.sub(/y\z/, "ie")}s", from: from
    select value, from: from
  end
end

RSpec.configure do |config|
  config.include SourceChipHelpers, type: :system
end
