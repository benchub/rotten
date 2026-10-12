# Turbo caches a snapshot of each page it leaves and shows it as a preview
# when a link goes back to that URL, then swaps in the page from the server.
# It drops data-turbo-preview before that swap, so the attribute can't tell
# the two apart. Instead, mark the body before leaving a page: the snapshot
# keeps the mark and a fresh body has none. Scope waits with fresh_body.
module TurboPreviewHelpers
  def fresh_body = "body:not([data-spec-left])"

  def mark_leaving = page.execute_script("document.body.dataset.specLeft = ''")
end

RSpec.configure do |config|
  config.include TurboPreviewHelpers, type: :system
end
