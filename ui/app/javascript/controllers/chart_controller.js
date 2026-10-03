import { Controller } from "@hotwired/stimulus"

// Hover and focus tooltips and drag-to-zoom for the server-drawn SVG charts
// (FingerprintsHelper#timeseries_chart). Everything it shows comes from the
// points' data attributes. It only toggles classes and sets attributes or
// CSSOM properties, never a style attribute, so it runs under the strict CSP.
export default class extends Controller {
  static targets = ["svg", "point", "guide", "selection", "tooltip"]
  static values = { label: String, zoomUrl: String }

  // Smaller drags, in screen pixels, are clicks and don't zoom.
  static minDrag = 4

  connect() {
    this.drag = null
    this.hide()
    this.selectionTarget.classList.add("chart-hidden")
  }

  start(event) {
    if (!this.hasZoomUrlValue || !this.pointTargets.length || event.button !== 0) return

    this.drag = { pointerId: event.pointerId, clientX: event.clientX, x: this.svgX(event) }
    this.svgTarget.setPointerCapture?.(event.pointerId)
  }

  move(event) {
    if (this.drag && event.pointerId === this.drag.pointerId) {
      if (this.dragged(event)) {
        const [left, right] = this.span(event)
        this.selectionTarget.setAttribute("x", left)
        this.selectionTarget.setAttribute("width", right - left)
        this.selectionTarget.classList.remove("chart-hidden")
      }
    }

    const point = this.nearest(this.svgX(event))
    if (point) this.show(point)
  }

  finish(event) {
    if (!this.drag || event.pointerId !== this.drag.pointerId) return

    const dragged = this.dragged(event)
    const [left, right] = this.span(event)
    this.cancel()
    if (!dragged) return

    // Each edge snaps to its nearest point, which clamps the range to the data.
    const first = this.nearest(left)
    const last = this.nearest(right)
    if (first && last) this.zoom(first.dataset.time, last.dataset.end)
  }

  cancel() {
    this.drag = null
    this.selectionTarget.classList.add("chart-hidden")
  }

  leave() {
    if (!this.drag) this.hide()
  }

  focus(event) {
    if (!this.pointTargets.includes(event.target)) return

    this.rove(event.target)
    this.show(event.target)
  }

  // Arrow keys, Home and End move between points; Tab leaves the chart.
  key(event) {
    const points = this.pointTargets
    const index = points.indexOf(event.target)
    if (index < 0 || event.altKey || event.ctrlKey || event.metaKey) return

    const next = {
      ArrowLeft: Math.max(index - 1, 0),
      ArrowRight: Math.min(index + 1, points.length - 1),
      Home: 0,
      End: points.length - 1
    }[event.key]
    if (next === undefined) return

    event.preventDefault()
    this.rove(points[next])
    points[next].focus()
  }

  // Makes point the chart's only Tab stop.
  rove(point) {
    for (const other of this.pointTargets) other.setAttribute("tabindex", other === point ? "0" : "-1")
  }

  blur() {
    this.hide()
  }

  show(point) {
    const x = point.getAttribute("cx")
    this.guideTarget.setAttribute("x1", x)
    this.guideTarget.setAttribute("x2", x)
    this.guideTarget.classList.remove("chart-hidden")

    this.tooltipTarget.textContent = this.text(point)
    const box = point.getBoundingClientRect()
    const origin = this.element.getBoundingClientRect()
    this.tooltipTarget.style.left = `${box.left + box.width / 2 - origin.left}px`
    this.tooltipTarget.classList.remove("chart-hidden")
  }

  hide() {
    this.guideTarget.classList.add("chart-hidden")
    this.tooltipTarget.classList.add("chart-hidden")
  }

  // Matches FingerprintsHelper#chart_point_label.
  text(point) {
    const number = Number(point.dataset.value)
    const value = Number.isFinite(number) && number > 0 ? number : 0
    const formatted = value.toLocaleString("en-US", { maximumFractionDigits: 2 })
    const time = point.dataset.time
    return `${this.labelValue}: ${formatted} at ${time.slice(0, 10)} ${time.slice(11, 16)} UTC`
  }

  // The first and last of the points nearest each end of the drag, so the
  // range stays inside the data and from is always before to. The server
  // re-renders it.
  zoom(from, to) {
    if (!from || !to || from >= to) return

    const url = new URL(this.zoomUrlValue, window.location.href)
    url.searchParams.set("range", "custom")
    url.searchParams.set("from", from.replace(/Z$/, ""))
    url.searchParams.set("to", to.replace(/Z$/, ""))
    if (window.Turbo) {
      window.Turbo.visit(url.toString())
    } else {
      window.location.assign(url.toString())
    }
  }

  dragged(event) {
    return Math.abs(event.clientX - this.drag.clientX) >= this.constructor.minDrag
  }

  span(event) {
    const x = this.svgX(event)
    return [Math.min(this.drag.x, x), Math.max(this.drag.x, x)]
  }

  nearest(x) {
    let best = null
    let distance = Infinity
    for (const point of this.pointTargets) {
      const d = Math.abs(this.pointX(point) - x)
      if (d < distance) {
        best = point
        distance = d
      }
    }
    return best
  }

  pointX(point) {
    return Number(point.getAttribute("cx"))
  }

  // From screen to viewBox coordinates.
  svgX(event) {
    const matrix = this.svgTarget.getScreenCTM()
    if (!matrix) return 0
    return new DOMPoint(event.clientX, event.clientY).matrixTransform(matrix.inverse()).x
  }
}
