# One fingerprint: its normalized SQL, and for a picked source and time range
# a time series chart, its top contexts and its stats on each role. The
# source and range go through the same ReportQuery validation as the
# fingerprint time series report, and every query runs through ReportRunner.
class FingerprintsController < ApplicationController
  def show
    @fingerprint = Fingerprint.lookup(params[:id])
    return render plain: "Not found", status: :not_found unless @fingerprint

    @query = ReportQuery.new(Report.find("fingerprint_timeseries"), query_params, catalog: LogicalSource.catalog)
    return unless @query.submitted?
    return render :show, status: :unprocessable_content unless @query.valid?

    runner = ReportRunner.new
    @series = @query.run(runner)
    @contexts = @query.run(runner, report: Report.internal("fingerprint_contexts"))
    @sources = @query.run(runner, report: Report.internal("fingerprint_sources"))
  rescue ActiveRecord::QueryCanceled
    @series = @contexts = @sources = nil
    @timeout_seconds = Rails.configuration.x.report_timeout_ms / 1000.0
    render :show, status: :service_unavailable
  end

  private

  # The fingerprint comes from the path, never from a fingerprint_id query
  # parameter. The chart is always in time order, so sort and dir are
  # dropped rather than carried into links.
  def query_params
    params.except(:fingerprint_id, :sort, :dir).merge(fingerprint_id: @fingerprint.id.to_s)
  end
end
