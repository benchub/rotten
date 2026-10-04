import { Controller } from "@hotwired/stimulus"

// Shows From and To only for a Custom time range, and disables them
// otherwise so the form doesn't send them. Switching from a preset to Custom
// fills them with that preset's window: it ends at the server's time when
// the page was drawn (the now value), as the preset's run did, and each
// preset option carries its length in data-seconds. The start is rounded
// down and the end up to the minute, so the custom range covers the preset's.
// Without JavaScript, From and To stay visible, labelled Custom range only.
export default class extends Controller {
  static targets = ["range", "custom", "from", "to"]
  static values = { now: String }

  connect() {
    this.previous = this.rangeTarget.value
    this.toggle()
  }

  change() {
    if (this.custom && this.previous !== "custom") this.prefill(this.previous)
    this.previous = this.rangeTarget.value
    this.toggle()
  }

  get custom() {
    return this.rangeTarget.value === "custom"
  }

  toggle() {
    this.customTarget.hidden = !this.custom
    this.fromTarget.disabled = !this.custom
    this.toTarget.disabled = !this.custom
  }

  prefill(preset) {
    const option = [...this.rangeTarget.options].find((o) => o.value === preset)
    const seconds = Number(option?.dataset.seconds)
    const now = Date.parse(this.nowValue)
    if (!seconds || Number.isNaN(now)) return
    const minute = 60 * 1000
    this.fromTarget.value = this.format(Math.floor((now - seconds * 1000) / minute) * minute)
    this.toTarget.value = this.format(Math.ceil(now / minute) * minute)
  }

  // A datetime-local value in UTC, to the minute.
  format(ms) {
    return new Date(ms).toISOString().slice(0, 16)
  }
}
