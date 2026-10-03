require "timeout"

class HealthController < ActionController::Base
  def show
    connection = nil

    Timeout.timeout(1) do
      connection = ActiveRecord::Base.connection
      connection.select_value("SELECT 1")
    end

    head :ok
  rescue ActiveRecord::ActiveRecordError, PG::Error, Timeout::Error
    discard_connection(connection)
    head :service_unavailable
  end

  private

  def discard_connection(connection)
    return unless connection

    ActiveRecord::Base.connection_pool.remove(connection)
    connection.disconnect!
  rescue StandardError
    nil
  end
end
