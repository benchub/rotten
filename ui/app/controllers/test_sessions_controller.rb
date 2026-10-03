class TestSessionsController < ApplicationController
  skip_before_action :require_login

  def create
    return head :not_found unless Rails.env.test?

    session[:user_id] = params.require(:user_id)
    redirect_to root_path
  end
end
