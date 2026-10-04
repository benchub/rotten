module ApplicationHelper
  APP_NAME = "rotten".freeze

  # The top bar's sections, by the controllers whose pages belong to them.
  NAV_SECTIONS = {
    reports: %w[reports fingerprints],
    admin: %w[admin api_keys audit_logs]
  }.freeze

  # The page's title, then the app's name.
  def page_title
    safe_join([content_for(:title).presence, APP_NAME].compact, " · ")
  end

  # A top bar link, marked as the current page's section.
  def nav_link(label, path, section)
    current = NAV_SECTIONS.fetch(section).include?(controller_name)
    link_to label, path, class: "topbar-link", aria: { current: ("page" if current) }
  end

  # A badge for a user's role.
  def role_badge(role)
    tag.span(role, class: ["badge", role == "admin" ? "badge-admin" : "badge-neutral"])
  end
end
