require Rails.root.join("lib/rotten_ui/auth_mode")

Rails.application.config.x.auth_mode = RottenUi::AuthMode.fetch!
