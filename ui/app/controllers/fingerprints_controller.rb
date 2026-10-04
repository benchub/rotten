# One fingerprint: its normalized SQL, and for a picked source and time range
# a time series chart, its top contexts and its stats on each role. The
# source and range go through the same ReportQuery validation as the
# fingerprint time series report, and every query runs through one
# ReportRunner, so the page shares one timeout.
class FingerprintsController < ApplicationController
  RANGE_FIELDS = %i[range from to].freeze

  def show
    @fingerprint = Fingerprint.lookup(params[:id])
    return render plain: "Not found", status: :not_found unless @fingerprint

    @query = ReportQuery.new(Report.find("fingerprint_timeseries"), query_params, catalog: LogicalSource.catalog)
    return unless @query.submitted?
    return render :show, status: :unprocessable_content unless @query.valid?

    runner = ReportRunner.new
    zoom_links
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

  # Zooming in on a chart goes to this page with a custom range, and keeps
  # the range from before the first zoom in reset_range, reset_from and
  # reset_to for the Reset zoom link. That range goes through the same
  # validation as the page's own; if it fails, it's ignored, as if the page
  # weren't zoomed.
  def zoom_links
    others = @query.link_params.except(:fingerprint_id, *RANGE_FIELDS)
    reset = reset_range(others)
    @reset_zoom_params = reset && others.merge(reset)
    before_zoom = reset || @query.link_params.slice(*RANGE_FIELDS)
    @zoom_url = fingerprint_path(@fingerprint.id, others.merge(before_zoom.transform_keys { |key| :"reset_#{key}" }))
  end

  def reset_range(others)
    range = params[:reset_range]
    return unless range.is_a?(String) && range.present?

    candidate = others.merge(range: range, fingerprint_id: @fingerprint.id.to_s)
    candidate.merge!(from: params[:reset_from], to: params[:reset_to]) if range.strip == "custom"
    query = ReportQuery.new(@query.report, candidate, catalog: @query.catalog)
    query.link_params.slice(*RANGE_FIELDS) if query.valid?
  end
end
