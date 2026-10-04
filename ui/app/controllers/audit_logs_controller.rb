# Reads rotten.ui_audit_log, newest first, a page at a time. Admins only.
class AuditLogsController < ApplicationController
  before_action :require_admin

  PAGE_SIZE = 50
  # A positive bigint, with no sign, padding or leading zero.
  BEFORE_FORMAT = /\A[1-9][0-9]{0,17}\z/

  def index
    before = params[:before]
    unless before.nil? || (before.is_a?(String) && before.match?(BEFORE_FORMAT))
      return head :bad_request
    end

    @paged = !before.nil?
    entries = UiAuditLog.page(before: before&.to_i, size: PAGE_SIZE + 1).to_a
    @older_before = entries[PAGE_SIZE - 1].id if entries.size > PAGE_SIZE
    @entries = entries.first(PAGE_SIZE)
  end
end
