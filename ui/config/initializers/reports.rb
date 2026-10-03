require Rails.root.join("lib/rotten_ui/report_timeout")

Rails.application.config.x.report_timeout_ms = RottenUi::ReportTimeout.fetch!

# The report SQL lives in the repo's reports/ directory, next to ui/. Docker
# images and the dev and test containers put it at /reports, which is next to
# the app too.
Rails.application.config.x.reports_dir = Rails.root.parent.join("reports").to_s

# Fail at boot, not on the first report request, when the SQL isn't there,
# for example an image built without the reports build context.
Rails.application.config.after_initialize do
  dir = Rails.application.config.x.reports_dir
  missing = Report.all.map(&:file).reject { |file| File.file?(File.join(dir, file)) }
  raise "report SQL missing from #{dir}: #{missing.join(', ')}" if missing.any?
end
