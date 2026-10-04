# /reports is the workbench: one form for the dataset (source and time
# window) and the report to run on it; the form keeps the dataset when
# another report is picked. /reports/:id is the old page for one report; it redirects
# to the workbench with every parameter kept, for bookmarks.
class ReportsController < ApplicationController
  def index
    key = params[:report]
    @report = key.nil? ? Report.default : Report.find(key)
    @query = ReportQuery.new(@report || Report.default, params, catalog: LogicalSource.catalog)
    unless @report
      @query.errors.add(:report, "is not one of the choices")
      return render :index, status: :unprocessable_content
    end
    return unless @query.submitted?
    @query.valid?
    # Opened without a fingerprint ID, as from an old bookmark: the form asks for one.
    @query.errors.delete(:fingerprint_id) if @query.needs_fingerprint?
    return render :index, status: :unprocessable_content if @query.errors.any?
    return if @query.needs_fingerprint?

    @rows = @query.run
  rescue ActiveRecord::QueryCanceled
    @timeout_seconds = Rails.configuration.x.report_timeout_ms / 1000.0
    render :index, status: :service_unavailable
  end

  def show
    report = Report.find(params[:id])
    return render plain: "Not found", status: :not_found unless report

    redirect_to reports_path(request.query_parameters.merge("report" => report.key)), status: :moved_permanently
  end
end
