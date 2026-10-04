namespace :users do
  # Prints the error and exits non-zero, so scripts notice.
  fail_with = lambda do |task, error|
    warn "#{task}: #{error.message}"
    exit 1
  end

  desc "Create a password user (role viewer or admin) and print a one-time password"
  task :create, %i[email role] => :environment do |task, args|
    created = UserAdmin.create(args[:email], args[:role])
    puts "Created #{created.user.role} #{created.user.email}."
    puts "Password: #{created.password}"
    puts "This is the only time the password is shown. Pass it on securely."
  rescue UserAdmin::Error => e
    fail_with.call(task.name, e)
  end

  desc "Disable a user, in either auth mode"
  task :disable, %i[email] => :environment do |task, args|
    user = UserAdmin.disable(args[:email])
    puts "Disabled #{user.email}. Their sessions end on their next request."
  rescue UserAdmin::Error => e
    fail_with.call(task.name, e)
  end

  desc "Enable a disabled user, in either auth mode"
  task :enable, %i[email] => :environment do |task, args|
    user = UserAdmin.enable(args[:email])
    puts "Enabled #{user.email}. They can sign in again."
  rescue UserAdmin::Error => e
    fail_with.call(task.name, e)
  end

  desc "Set and print a new password for a password user"
  task :reset_password, %i[email] => :environment do |task, args|
    reset = UserAdmin.reset_password(args[:email])
    puts "New password for #{reset.user.email}."
    puts "Password: #{reset.password}"
    puts "This is the only time the password is shown. Pass it on securely."
  rescue UserAdmin::Error => e
    fail_with.call(task.name, e)
  end
end
