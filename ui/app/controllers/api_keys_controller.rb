# Pass key admin: list, create and revoke worker pass keys. Admins only.
class ApiKeysController < ApplicationController
  before_action :require_admin

  def index
    @api_keys = ApiKey.order(id: :desc)
  end

  def new
    @issue = ApiKeyIssue.new
  end

  # The token is rendered straight into this response, never redirected,
  # flashed or stored, and the response is marked no-store, so this page is
  # the only time anyone sees the secret.
  def create
    @issue = ApiKeyIssue.new(params.expect(api_key: %i[name fqdn]))
    @issued = @issue.save(current_user)
    return render :new, status: :unprocessable_content unless @issued

    no_store
    render :created, status: :created
  end

  def revoke
    api_key = ApiKey.find(params[:id])
    notice = api_key.revoke!(current_user) ? "Revoked #{api_key.name}." : "#{api_key.name} was already revoked."
    redirect_to api_keys_path, notice: notice
  end
end
