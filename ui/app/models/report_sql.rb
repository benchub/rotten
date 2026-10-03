# Reads report SQL from the reports directory. Only the files the Report
# registry names can be read.
module ReportSql
  def self.read(file)
    raise ArgumentError, "not a report file: #{file.inspect}" unless Report.all.any? { |report| report.file == file }

    File.read(File.join(Rails.configuration.x.reports_dir, file))
  end
end
