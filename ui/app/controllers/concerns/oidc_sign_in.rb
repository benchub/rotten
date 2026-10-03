module OidcSignIn
  extend ActiveSupport::Concern

  DENIALS = {
    invalid: "Sign-in didn't work. Try again, or ask your administrator for help.",
    missing_email: "Your identity provider didn't share an email address, so Rotten can't sign you in. " \
                   "Ask your administrator to release the email claim to Rotten.",
    not_authorized: "You're signed in with your identity provider, but you're not in a group that can use Rotten. " \
                    "Ask your administrator for access.",
    inactive: "Your Rotten account is disabled. Ask your administrator for help.",
    unverified_email: "Your identity provider hasn't verified your email address, so Rotten can't sign you in. " \
                      "Verify your email with your identity provider, or ask your administrator for help.",
    conflict: "Your sign-in couldn't be matched to a Rotten account. Ask your administrator for help."
  }.freeze

  private

  def sign_in_with_oidc(auth, provider:)
    result = OidcLogin.new(config: Rails.configuration.x.oidc, provider: provider).call(auth)

    # A new session either way: fixation protection on success, and a denied
    # login ends whatever session was there before.
    reset_session
    @current_user = nil

    if result.user
      session[:user_id] = result.user.id
      redirect_to root_path, notice: "Signed in"
    else
      Rails.logger.info("OIDC sign-in refused: #{result.error}")
      render "sessions/denied", status: :forbidden, locals: { message: DENIALS.fetch(result.error) }
    end
  end
end
