import { Controller } from "@hotwired/stimulus"

// Shows only the fields the picked report reads. Each field target names
// the reports that read it in data-reports; the others are hidden and their
// inputs disabled, so the form doesn't send them. A field marked
// data-report-chooser-keep, the dataset role, is hidden but still sent, so
// the reports that ignore it pass it on. Without JavaScript every
// field shows and the server ignores the ones the report doesn't read.
export default class extends Controller {
  static targets = ["choice", "field"]

  connect() {
    this.update()
  }

  update() {
    const picked = this.choiceTargets.find((choice) => choice.checked)?.value
    if (!picked) return
    for (const field of this.fieldTargets) {
      const on = field.dataset.reports.split(" ").includes(picked)
      field.hidden = !on
      if (field.dataset.reportChooserKeep === "true") continue
      for (const input of field.querySelectorAll("input, select, textarea")) input.disabled = !on
    }
  }
}
