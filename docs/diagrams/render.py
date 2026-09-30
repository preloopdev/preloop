#!/usr/bin/env python3
"""Render docs/diagrams/*.json to SVG + single-diagram HTML (repo-local format).

Element types: rectangle (with optional {label:{text,fontSize}}), text, arrow
(polyline `points` relative to x/y, optional {label}, optional strokeDasharray),
cameraUpdate (canvas size).
"""
import html, json, re, sys
from pathlib import Path

FONT = "system-ui, sans-serif"
MARKER_COLORS = {
    "#1e1e1e": "ah-dark", "#15803d": "ah-green", "#7c3aed": "ah-purple",
    "#b45309": "ah-amber", "#0f766e": "ah-teal", "#4a9eed": "ah-blue",
    "#dc2626": "ah-red", "#ec4899": "ah-pink", "#64748b": "ah-gray",
    "#b0b0b0": "ah-lgray", "#f59e0b": "ah-orange", "#2563eb": "ah-blue2",
    "#60a5fa": "ah-sky", "#10b981": "ah-emerald", "#8b5cf6": "ah-violet",
}
MARKER_FALLBACK = "ah-dark"


def text_width(txt, size):
    return len(txt) * size * 0.6


def esc(t):
    return html.escape(t, quote=True)


def render(elements):
    cam = next((e for e in elements if e.get("type") == "cameraUpdate"), {})
    w, h = cam.get("width", 1600), cam.get("height", 1200)
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" '
           f'width="{w}" height="{h}" style="background:#fff">']
    used = {MARKER_COLORS.get(e.get("strokeColor"), MARKER_FALLBACK)
            for e in elements if e.get("type") == "arrow"}
    out.append("<defs>")
    for name, color in ((n, c) for c, n in MARKER_COLORS.items() if n in used):
        out.append(
            f'  <marker id="{name}" viewBox="0 0 10 10" refX="8" refY="5" '
            f'markerWidth="6" markerHeight="6" orient="auto-start-reverse">'
            f'<path d="M 0 1.5 L 10 5 L 0 8.5 z" fill="{color}" /></marker>')
    out.append("</defs>")

    for e in elements:
        t = e.get("type")
        if t == "cameraUpdate":
            continue
        if t == "rectangle":
            x, y, rw, rh = e["x"], e["y"], e["width"], e["height"]
            fill = e.get("backgroundColor", "none")
            op = e.get("opacity", 100)
            opattr = f' fill-opacity="{op/100:g}"' if op < 100 else ""
            dash = ' stroke-dasharray="6,4"' if e.get("strokeDasharray") else ""
            rx = 10 if e.get("roundness") else 0
            out.append(
                f'<rect x="{x}" y="{y}" width="{rw}" height="{rh}" rx="{rx}" '
                f'fill="{fill}"{opattr} stroke="{e.get("strokeColor", "none")}" '
                f'stroke-width="{e.get("strokeWidth", 1)}"{dash}/>')
            lab = (e.get("label") or {}).get("text")
            if lab:
                fs = (e.get("label") or {}).get("fontSize", 14)
                lines = lab.split("\n")
                lh = fs * 1.25
                y0 = y + rh / 2 - (len(lines) - 1) * lh / 2 + fs * 0.35
                for i, ln in enumerate(lines):
                    out.append(
                        f'<text x="{x + rw/2:g}" y="{y0 + i*lh:.1f}" '
                        f'text-anchor="middle" font-size="{fs}" '
                        f'font-family="{FONT}" fill="{e.get("strokeColor", "#1e1e1e")}" '
                        f'font-weight="600">{esc(ln)}</text>')
        elif t == "text":
            fs = e.get("fontSize", 14)
            weight = "500" if fs >= 18 else "400"
            for i, ln in enumerate(e.get("text", "").split("\n")):
                out.append(
                    f'<text x="{e["x"]}" y="{e["y"] + fs + i*fs*1.25:.1f}" '
                    f'font-size="{fs}" font-family="{FONT}" '
                    f'fill="{e.get("strokeColor", "#1e1e1e")}" '
                    f'font-weight="{weight}">{esc(ln)}</text>')
        elif t == "arrow":
            pts = [(e["x"] + p[0], e["y"] + p[1]) for p in e.get("points", [[0, 0]])]
            d = "M " + " L ".join(f"{px:g},{py:g}" for px, py in pts)
            color = e.get("strokeColor", "#1e1e1e")
            sw = e.get("strokeWidth", 2)
            dash = ' stroke-dasharray="6,4"' if e.get("strokeDasharray") else ""
            marker = ""
            if e.get("endArrowhead") not in (None, "none"):
                marker = f' marker-end="url(#{MARKER_COLORS.get(color, MARKER_FALLBACK)})"'
            out.append(f'<path d="{d}" stroke="{color}" stroke-width="{sw}"{dash}{marker} fill="none"/>')
            lab = (e.get("label") or {}).get("text")
            if lab and len(pts) >= 2:
                fs = (e.get("label") or {}).get("fontSize", 13)
                # label anchor: midpoint of the longest segment
                best, bl = (pts[0], pts[1]), -1
                for a, b in zip(pts, pts[1:]):
                    seg = ((b[0]-a[0])**2 + (b[1]-a[1])**2) ** 0.5
                    if seg > bl:
                        best, bl = (a, b), seg
                (ax, ay), (bx, by) = best
                mx, my = (ax + bx) / 2, (ay + by) / 2
                tw = text_width(lab, fs)
                out.append(
                    f'<rect x="{mx - tw/2 - 6:.1f}" y="{my - fs*0.9:.1f}" '
                    f'width="{tw + 12:.1f}" height="{fs + 6:.1f}" rx="4" '
                    f'fill="#ffffff" fill-opacity="0.9" stroke="#e0e0e0" stroke-width="0.5"/>')
                out.append(
                    f'<text x="{mx:.1f}" y="{my + fs*0.35:.1f}" text-anchor="middle" '
                    f'font-size="{fs}" font-family="{FONT}" fill="{color}" '
                    f'font-weight="600">{esc(lab)}</text>')
        elif t == "line":
            pts = [(e["x"] + p[0], e["y"] + p[1]) for p in e.get("points", [[0, 0]])]
            d = "M " + " L ".join(f"{px:g},{py:g}" for px, py in pts)
            dash = ' stroke-dasharray="6,4"' if e.get("strokeDasharray") else ""
            out.append(f'<path d="{d}" stroke="{e.get("strokeColor", "#1e1e1e")}" '
                       f'stroke-width="{e.get("strokeWidth", 1.5)}"{dash} fill="none"/>')
        elif t == "ellipse":
            rx, ry = e.get("width", 20) / 2, e.get("height", 20) / 2
            out.append(
                f'<ellipse cx="{e["x"] + rx:g}" cy="{e["y"] + ry:g}" rx="{rx:g}" ry="{ry:g}" '
                f'fill="{e.get("backgroundColor", "none")}" '
                f'stroke="{e.get("strokeColor", "none")}" '
                f'stroke-width="{e.get("strokeWidth", 1)}"/>')
            lab = (e.get("label") or {}).get("text")
            if lab:
                fs = (e.get("label") or {}).get("fontSize", 13)
                out.append(
                    f'<text x="{e["x"] + rx:g}" y="{e["y"] + ry + fs*0.35:.1f}" '
                    f'text-anchor="middle" font-size="{fs}" font-family="{FONT}" '
                    f'fill="{e.get("strokeColor", "#1e1e1e")}">{esc(lab)}</text>')
    out.append("</svg>")
    return "\n".join(out)


HTML = ('<!DOCTYPE html><html><head><meta charset="utf-8"><title>{name}</title>'
        '<style>body{{margin:0;background:#fff;display:flex;align-items:center;'
        'justify-content:center;min-height:100vh}}svg{{max-width:100%;height:auto}}</style>'
        '</head><body>\n{svg}\n</body></html>\n')


def main():
    root = Path(__file__).resolve().parent
    for jp in sorted(root.glob("*.json")):
        elements = json.loads(jp.read_text())
        if isinstance(elements, dict):
            elements = elements.get("elements", [])
        svg = render(elements)
        (jp.with_suffix(".svg")).write_text(svg + "\n")
        (jp.with_suffix(".html")).write_text(
            HTML.format(name=jp.stem, svg=svg))
        print("rendered", jp.name)
    # rebuild index.html embedding every svg
    parts = ['<!DOCTYPE html>\n<html lang="en"><head><meta charset="UTF-8">\n'
             '<title>Preloop — Architecture Diagrams</title>\n<style>\n'
             '*{box-sizing:border-box;margin:0;padding:0}html,body{height:100%}\n'
             'body{font-family:system-ui,sans-serif;background:#fafafa;display:grid;\n'
             'grid-template-columns:280px 1fr;height:100vh;overflow:hidden}\n'
             'nav{background:#1e1e1e;color:#e5e5e5;padding:24px 16px;overflow-y:auto}\n'
             'nav h1{font\n\n']
    old = (root / "index.html").read_text()
    head_end = old.find('<main id="host">')
    parts = [old[:head_end] if head_end > 0 else parts[0]]
    parts.append('<main id="host">\n')
    for jp in sorted(root.glob("*.json")):
        svg = jp.with_suffix(".svg").read_text().rstrip()
        parts.append(f'<div class="dia" id="d_{jp.stem}">{svg}</div>\n')
    parts.append("</main>\n</body></html>\n")
    (root / "index.html").write_text("".join(parts))
    print("index.html rebuilt")


if __name__ == "__main__":
    main()
