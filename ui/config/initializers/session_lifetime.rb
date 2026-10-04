require Rails.root.join("lib/rotten_ui/session_lifetime")

Rails.application.config.x.session_lifetime_seconds = RottenUi::SessionLifetime.fetch!
