class HomeController < ApplicationController
  # One read-only REPEATABLE READ snapshot, so a refresh that commits between
  # the two reads can't pair one refresh's stats with another's heat map.
  def show
    HomeStats.transaction(isolation: :repeatable_read) do
      HomeStats.connection.execute("set transaction read only")
      @stats = HomeStats.current
      @heatmap = @stats&.heatmap
    end
  end
end
