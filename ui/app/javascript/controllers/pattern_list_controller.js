import { Controller } from "@hotwired/stimulus"

// A list of regex patterns (Match or Exclude) as chips. The server renders
// each chosen pattern as a text input of its own, so without JavaScript they
// can be edited or cleared. This turns each into a chip: the pattern as text,
// a hidden input that still submits it, and a labeled remove button. Enter in
// the entry field, or the Add button, adds the typed pattern as a chip; a
// pattern left typed in the entry field is submitted with the form too.
// Every element is built with the DOM API, so it runs under the strict CSP.
export default class extends Controller {
  static targets = ["list", "input", "add"]
  static values = { noun: String }

  connect() {
    this.name = this.inputTarget.name
    for (const input of this.listTarget.querySelectorAll("input.pattern-value")) {
      input.closest("li").replaceWith(this.chip(input.value))
    }
    this.addTarget.hidden = false
    this.refresh()
  }

  key(event) {
    if (event.key !== "Enter" || this.inputTarget.value === "") return
    event.preventDefault()
    this.add()
  }

  add() {
    const value = this.inputTarget.value
    if (value !== "" && !this.patterns().includes(value)) this.listTarget.append(this.chip(value))
    this.inputTarget.value = ""
    this.inputTarget.focus()
    this.refresh()
  }

  patterns() {
    return [...this.listTarget.querySelectorAll("input[type=hidden]")].map((input) => input.value)
  }

  chip(value) {
    const item = document.createElement("li")
    item.className = "pattern-chip"
    const text = document.createElement("code")
    text.textContent = value
    const input = document.createElement("input")
    input.type = "hidden"
    input.name = this.name
    input.value = value
    const button = document.createElement("button")
    button.type = "button"
    button.className = "source-chip-remove"
    const label = `Remove ${this.nounValue} pattern ${value}`
    button.setAttribute("aria-label", label)
    button.title = label
    button.textContent = "×"
    button.addEventListener("click", () => {
      item.remove()
      this.refresh()
      this.inputTarget.focus()
    })
    item.append(text, input, button)
    return item
  }

  refresh() {
    this.listTarget.hidden = this.patterns().length === 0
  }
}
