import { Controller } from "@hotwired/stimulus"

// Turns each source field (project, environment, cluster, role), rendered as
// a multi-select of every value, into a dropdown and a list of chips. The
// multi-select stays in the form, hidden, and holds the chosen values, so the
// form submits them as repeated parameters (cluster[]=...).
//
// Picking a value adds it as a chip; picking "All" removes the field's chips.
// Each chip has a remove button. A field's dropdown offers only the values
// some source has under the chips of the fields before it, the same
// narrowing the server applies. A chip the earlier fields rule out stays,
// marked stale: the server ignores it, and says so. A field
// whose chips are all stale matches nothing.
//
// The catalog comes from a data attribute and every element is built with
// the DOM API, so it runs under the strict CSP. Without JavaScript the
// multi-selects stay as they are.
export default class extends Controller {
  static targets = ["values"]
  static values = { catalog: Array }

  connect() {
    this.fields = this.valuesTargets.map((values) => this.enhance(values))
    this.render()
  }

  disconnect() {
    for (const field of this.fields || []) {
      field.picker.remove()
      field.chips.remove()
      field.values.id = field.id
      field.values.hidden = false
      field.hint.hidden = false
    }
    this.fields = []
  }

  enhance(values) {
    const id = values.id
    const picker = document.createElement("select")
    picker.id = id
    picker.className = "source-select"
    const chips = document.createElement("ul")
    chips.className = "source-chips"
    chips.setAttribute("aria-label", `Chosen ${values.dataset.noun} filters`)
    values.id = `${id}-values`
    values.hidden = true
    const hint = values.parentElement.querySelector(".source-hint") || document.createElement("p")
    hint.hidden = true
    values.after(picker, chips)
    const field = { id, values, picker, chips, hint, level: Number(values.dataset.level),
                    noun: values.dataset.noun, allLabel: values.dataset.allLabel }
    picker.addEventListener("change", () => this.pick(field))
    return field
  }

  chosen(field) {
    return [...field.values.options].filter((option) => option.selected).map((option) => option.value)
  }

  pick(field) {
    const value = field.picker.value
    if (value === "") {
      for (const option of field.values.options) option.selected = false
    } else if (value !== PLACEHOLDER) {
      let option = [...field.values.options].find((o) => o.value === value)
      if (!option) {
        option = new Option(value, value)
        field.values.add(option)
      }
      option.selected = true
    }
    this.render()
    field.picker.focus()
  }

  remove(field, value) {
    const option = [...field.values.options].find((o) => o.value === value)
    if (option) option.selected = false
    this.render()
    // The chip's button is gone; keep focus in the field.
    const next = field.chips.querySelector("button") || field.picker
    next.focus()
  }

  // Rebuilds every field's dropdown and chips from the chosen values. Each
  // field narrows the catalog to its live chips, in order.
  render() {
    let rows = this.catalogValue
    for (const field of this.fields) {
      const present = [...new Set(rows.map((row) => row[field.level]))]
      const chosen = this.chosen(field)
      const live = chosen.filter((value) => present.includes(value))
      this.fillPicker(field, present, chosen)
      this.fillChips(field, chosen, live)
      // A field with chips filters on its live ones; with none live, on nothing.
      if (chosen.length > 0) rows = rows.filter((row) => live.includes(row[field.level]))
    }
  }

  fillPicker(field, present, chosen) {
    const options = [new Option(field.allLabel, "")]
    if (chosen.length > 0) {
      // Shows how many are chosen, and makes picking All again a change.
      const placeholder = new Option(`${chosen.length} chosen`, PLACEHOLDER)
      placeholder.hidden = true
      options.unshift(placeholder)
    }
    for (const value of present) {
      if (!chosen.includes(value)) options.push(new Option(value, value))
    }
    field.picker.replaceChildren(...options)
    field.picker.value = chosen.length > 0 ? PLACEHOLDER : ""
  }

  fillChips(field, chosen, live) {
    const items = chosen.map((value) => {
      const stale = !live.includes(value)
      const item = document.createElement("li")
      item.className = stale ? "source-chip source-chip-stale" : "source-chip"
      const label = document.createElement("span")
      label.textContent = value
      item.append(label)
      if (stale) {
        const note = document.createElement("span")
        note.className = "source-chip-note"
        note.textContent = "(ignored: not under the other filters)"
        item.append(note)
      }
      const button = document.createElement("button")
      button.type = "button"
      button.className = "source-chip-remove"
      button.setAttribute("aria-label", `Remove ${field.noun} ${value}`)
      button.title = button.getAttribute("aria-label")
      button.textContent = "×"
      button.addEventListener("click", () => this.remove(field, value))
      item.append(button)
      return item
    })
    field.chips.replaceChildren(...items)
    field.chips.hidden = items.length === 0
  }
}

// The dropdown's value while chips are chosen. No source has it: catalog
// values can't hold a NUL.
const PLACEHOLDER = "\u0000chosen"
