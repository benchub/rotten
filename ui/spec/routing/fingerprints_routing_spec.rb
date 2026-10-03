require "rails_helper"

# The fingerprint id route takes any single path segment, so malformed ids
# reach the controller and get its own 404 rather than a routing error.
RSpec.describe "Fingerprint routes", type: :routing do
  it "routes any single segment to fingerprints#show" do
    ["123", "abc", "1.5", "-1", "1%3Bselect%201", "%3Cscript%3E"].each do |id|
      expect(get: "/fingerprints/#{id}").to route_to("fingerprints#show", id: CGI.unescape(id))
    end
  end

  it "doesn't route nested paths" do
    expect(get: "/fingerprints/1/2").not_to be_routable
  end
end
