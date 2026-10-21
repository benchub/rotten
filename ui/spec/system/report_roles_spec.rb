require "rails_helper"

# The utilization reports offer and accept only roles some source has. A
# deployment that names its roles differently sees no "replica" choice.
RSpec.describe "Utilization report roles", type: :system do
  def seed_roles(*roles)
    ReportFixture.seed!
    conn = ReportFixture.connect
    ReportFixture.clear(conn)
    roles.each do |role|
      conn.exec_params("insert into rotten.logical_sources (project, environment, cluster, role) values ($1, $2, $3, $4)",
                       ["canvas", "production", "13", role])
    end
    conn.close
    User.delete_all
    user = User.create!(email: "roles-viewer@example.com", name: "Viewer", role: "viewer", active: true)
    visit "/__test/sign_in?user_id=#{user.id}"
  end

  context "when no source has a replica role" do
    before { seed_roles("primary", "reader") }

    it "offers only real roles, preselects primary, and leaves Replica unselected" do
      visit "/reports"
      choose "Replica utilization by job"

      expect(page).to have_select("Primary role", options: %w[primary reader], selected: "primary")
      expect(page).to have_select("Replica role", options: ["", "primary", "reader"], selected: "")
    end

    it "rejects a replica role no source has" do
      visit "/reports?report=replica_utilization_by_job&range=3h&primary_role=primary&replica_role=replica"

      expect(page).to have_css(".report-errors", text: "Replica role is not a known role")
      expect(page).to have_select("Replica role", options: ["", "primary", "reader"], selected: "")
    end

    it "keeps an explicitly blank role blank and asks for a choice" do
      visit "/reports?report=replica_utilization_by_job&range=3h&primary_role=&replica_role=reader"

      expect(page).to have_select("Primary role", options: ["", "primary", "reader"], selected: "")
      expect(page).to have_css(".report-errors li", count: 1)
      expect(page).to have_css(".report-errors", text: "Primary role must be chosen")
      expect(page).to have_no_text("must differ")
    end

    it "lists every source's role in the pickers even when the role filter narrows the dataset" do
      visit "/reports?report=replica_utilization_by_job&range=3h&role%5B%5D=primary&primary_role=primary&replica_role=reader"

      expect(page).to have_no_css(".report-errors")
      expect(page).to have_select("Primary role", options: %w[primary reader], selected: "primary", visible: :all)
      expect(page).to have_select("Replica role", options: %w[primary reader], selected: "reader", visible: :all)
    end

    it "requires a replica role to be chosen" do
      visit "/reports?report=replica_utilization_by_job&range=3h"

      expect(page).to have_css(".report-errors", text: "Replica role must be chosen")
    end
  end

  context "when every source has the same role" do
    before { seed_roles("primary") }

    it "rejects that role for both" do
      visit "/reports?report=replica_utilization_by_job&range=3h&primary_role=primary&replica_role=primary"

      expect(page).to have_css(".report-errors", text: "Replica role must differ from Primary role")
    end
  end

  context "when no source has a primary role" do
    before { seed_roles("writer", "replica") }

    it "leaves Primary unselected and preselects replica" do
      visit "/reports"
      choose "Replica utilization by controller and action"

      expect(page).to have_select("Primary role", options: ["", "replica", "writer"], selected: "")
      expect(page).to have_select("Replica role", options: %w[replica writer], selected: "replica")
    end
  end
end
