# app/sql holds the SQL the models run, not Ruby, so its directories aren't
# namespaces.
Rails.autoloaders.main.ignore(Rails.root.join("app/sql"))
