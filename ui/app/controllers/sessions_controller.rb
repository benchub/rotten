class SessionsController < ApplicationController
  skip_before_action :require_login, only: :new

  def new
  end

  def destroy
    reset_session
    redirect_to login_path, notice: "Signed out"
  end
end
