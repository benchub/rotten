import { Controller } from "@hotwired/stimulus"

// Marks the report form busy from the moment it's submitted: aria-busy on
// the form, which shows a spinner (reports.css), and Run report disabled
// against double submits. "Running report…" goes in a status region outside
// the form, since assistive tech may hold back announcements inside a busy
// element. The form is a full-page GET, so the next page, with results, an
// error or the timeout message, starts idle. A page restored from the
// back-forward cache is reset on pageshow.
export default class extends Controller {
  static targets = ["form", "button", "status"]

  start() {
    this.formTarget.setAttribute("aria-busy", "true")
    this.buttonTarget.disabled = true
    this.statusTarget.textContent = "Running report…"
  }

  reset() {
    this.formTarget.removeAttribute("aria-busy")
    this.buttonTarget.disabled = false
    this.statusTarget.textContent = ""
  }
}
