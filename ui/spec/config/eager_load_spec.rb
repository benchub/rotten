require "rails_helper"

RSpec.describe "eager loading" do
  it "loads every autoloaded file the way production does" do
    expect { Rails.application.eager_load! }.not_to raise_error
  end
end
