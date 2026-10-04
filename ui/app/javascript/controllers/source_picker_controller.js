import { Controller } from "@hotwired/stimulus"

// Narrows the environment, cluster and role dropdowns to the sources that
// exist for the choices before them. When a choice changes, a later choice
// that no longer exists falls back to the first option, or to All roles.
// Choices the page rendered as selected, and those at or before the one
// changed, are kept even if they don't exist, so the form still matches the
// server's error. The catalog comes from a data attribute and options are built with
// the DOM API, so it runs under the strict CSP. Without JavaScript the full
// lists stay and the server validates the combination.
export default class extends Controller {
  static targets = ["project", "environment", "cluster", "role"]
  static values = { catalog: Array }

  connect() {
    this.narrow()
  }

  narrow(event) {
    const levels = [this.projectTarget, this.environmentTarget, this.clusterTarget, this.hasRoleTarget ? this.roleTarget : null]
    const changed = event ? levels.indexOf(event.target) : -1
    const keep = (level) => event ? level <= changed : levels[level].selectedOptions[0]?.defaultSelected === true
    const project = this.projectTarget.value
    const rows = this.catalogValue.filter((row) => row[0] === project)
    const environment = this.fill(this.environmentTarget, rows.map((row) => row[1]), keep(1))
    const inEnvironment = rows.filter((row) => row[1] === environment)
    const cluster = this.fill(this.clusterTarget, inEnvironment.map((row) => row[2]), keep(2))
    if (this.hasRoleTarget) {
      const roles = inEnvironment.filter((row) => row[2] === cluster).map((row) => row[3])
      this.fill(this.roleTarget, roles, keep(3), this.roleTarget.options[0])
    }
  }

  // Replaces select's options with values and returns its choice. The
  // current choice stays if it's one of them, or if keep is set. The
  // rendered choice stays marked, for when the controller reconnects.
  fill(select, values, keep, blank = null) {
    const current = select.value
    const rendered = [...select.options].find((option) => option.defaultSelected)?.value
    const unique = [...new Set(values)]
    if (keep && current !== "" && !unique.includes(current)) unique.push(current)
    const options = unique.map((value) => new Option(value, value, value === rendered))
    if (blank) options.unshift(new Option(blank.text, blank.value, blank.value === rendered))
    select.replaceChildren(...options)
    if (unique.includes(current)) select.value = current
    return select.value
  }
}
