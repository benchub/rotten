class ReportsController < ApplicationController
  def index
    @reports = Report.all
  end

  def show
    @report = Report.find(params[:id])
    return render plain: "Not found", status: :not_found unless @report

    @query = ReportQuery.new(@report, params, catalog: LogicalSource.catalog)
    return unless @query.submitted?
    return render :show, status: :unprocessable_content unless @query.valid?

    @rows = @query.run
  rescue ActiveRecord::QueryCanceled
    @timeout_seconds = Rails.configuration.x.report_timeout_ms / 1000.0
    render :show, status: :service_unavailable
  end
end
