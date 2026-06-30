#!/usr/bin/env python3
"""Local Excalidraw-to-SVG-to-PNG compiler.

Compiles Excalidraw JSON elements directly into a standard SVG file,
then uses Playwright to snapshot it as a PNG. No external web CDNs needed.
"""

import argparse
import json
import math
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from playwright.sync_api import sync_playwright


def get_bounding_box(elements):
    if not elements:
        return 0, 0, 800, 600
    min_x = float('inf')
    min_y = float('inf')
    max_x = float('-inf')
    max_y = float('-inf')
    for e in elements:
        ex = e.get("x", 0.0)
        ey = e.get("y", 0.0)
        ew = e.get("width", 0.0)
        eh = e.get("height", 0.0)
        etype = e.get("type")
        if etype in ("line", "arrow") and "points" in e:
            for pt in e["points"]:
                px = ex + pt[0]
                py = ey + pt[1]
                min_x = min(min_x, px)
                min_y = min(min_y, py)
                max_x = max(max_x, px)
                max_y = max(max_y, py)
        else:
            min_x = min(min_x, ex)
            min_y = min(min_y, ey)
            max_x = max(max_x, ex + ew)
            max_y = max(max_y, ey + eh)
    return min_x, min_y, max_x, max_y


def build_svg(data):
    elements = [e for e in data.get("elements", []) if not e.get("isDeleted")]
    min_x, min_y, max_x, max_y = get_bounding_box(elements)
    
    padding = 50
    width = int(max_x - min_x + padding * 2)
    height = int(max_y - min_y + padding * 2)
    
    # Create SVG root
    svg = ET.Element("svg", {
        "xmlns": "http://www.w3.org/2000/svg",
        "width": str(width),
        "height": str(height),
        "viewBox": f"{int(min_x - padding)} {int(min_y - padding)} {width} {height}",
        "style": "background-color: #ffffff;"
    })

    # Add marker defs for arrowheads
    defs = ET.SubElement(svg, "defs")
    # Loop over unique stroke colors to define arrowhead markers
    colors = set()
    for e in elements:
        if e.get("type") == "arrow" and e.get("strokeColor"):
            colors.add(e["strokeColor"])
            
    for color in colors:
        safe_id = "arrow-" + color.replace("#", "")
        marker = ET.SubElement(defs, "marker", {
            "id": safe_id,
            "viewBox": "0 0 10 10",
            "refX": "6",
            "refY": "5",
            "markerWidth": "6",
            "markerHeight": "6",
            "orient": "auto"
        })
        ET.SubElement(marker, "path", {
            "d": "M 0 0 L 10 5 L 0 10 z",
            "fill": color
        })

    for e in elements:
        etype = e.get("type")
        x = e.get("x", 0)
        y = e.get("y", 0)
        w = e.get("width", 0)
        h = e.get("height", 0)
        sc = e.get("strokeColor", "#000000")
        bc = e.get("backgroundColor", "transparent")
        sw = e.get("strokeWidth", 2)
        ss = e.get("strokeStyle", "solid")
        
        # Stroke style mapping
        dash = None
        if ss == "dashed":
            dash = "6,6"
        elif ss == "dotted":
            dash = "2,4"
            
        style_attrs = {
            "stroke": sc,
            "stroke-width": str(sw),
        }
        if bc and bc != "transparent":
            style_attrs["fill"] = bc
        else:
            style_attrs["fill"] = "none"
            
        if dash:
            style_attrs["stroke-dasharray"] = dash

        if etype == "rectangle":
            rect_attrs = style_attrs.copy()
            rect_attrs.update({
                "x": str(x),
                "y": str(y),
                "width": str(w),
                "height": str(h)
            })
            if e.get("roundness"):
                rect_attrs["rx"] = "8"
                rect_attrs["ry"] = "8"
            ET.SubElement(svg, "rect", rect_attrs)

        elif etype == "ellipse":
            ell_attrs = style_attrs.copy()
            ell_attrs.update({
                "cx": str(x + w / 2),
                "cy": str(y + h / 2),
                "rx": str(w / 2),
                "ry": str(h / 2)
            })
            ET.SubElement(svg, "ellipse", ell_attrs)

        elif etype == "line" or etype == "arrow":
            points = e.get("points", [])
            if len(points) >= 2:
                # Build SVG path representation
                d_parts = []
                for idx, pt in enumerate(points):
                    px = x + pt[0]
                    py = y + pt[1]
                    d_parts.append(f"{'M' if idx == 0 else 'L'} {px} {py}")
                
                path_attrs = style_attrs.copy()
                path_attrs["d"] = " ".join(d_parts)
                
                if etype == "arrow":
                    # Attach the marker matching the stroke color
                    safe_id = "arrow-" + sc.replace("#", "")
                    path_attrs["marker-end"] = f"url(#{safe_id})"
                    
                ET.SubElement(svg, "path", path_attrs)

        elif etype == "text":
            text_str = e.get("text", "")
            fs = e.get("fontSize", 16)
            ta = e.get("textAlign", "center")
            
            # Simple text rendering
            text_attrs = {
                "font-family": "system-ui, sans-serif",
                "font-size": f"{fs}px",
                "fill": sc,
                "dominant-baseline": "hanging"
            }
            
            # Text alignment mapping
            if ta == "center":
                text_attrs["text-anchor"] = "middle"
                tx = x + w / 2
            elif ta == "right":
                text_attrs["text-anchor"] = "end"
                tx = x + w
            else:
                text_attrs["text-anchor"] = "start"
                tx = x

            lines = text_str.split("\n")
            # Excalidraw text layout starting y
            ty = y
            if e.get("containerId"):
                # If bound to a container shape, vertically center the lines
                line_height = fs * 1.3
                total_h = len(lines) * line_height
                ty = y + (h - total_h) / 2
                
            text_attrs["x"] = str(tx)
            text_attrs["y"] = str(ty)
            
            text_node = ET.SubElement(svg, "text", text_attrs)
            
            for line_idx, line in enumerate(lines):
                tspan_attrs = {
                    "x": str(tx),
                    "dy": f"{0 if line_idx == 0 else 1.3}em"
                }
                tspan = ET.SubElement(text_node, "tspan", tspan_attrs)
                tspan.text = line

    return ET.tostring(svg, encoding="utf-8")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("input", type=Path)
    ap.add_argument("-o", "--output", type=Path, default=None)
    args = ap.parse_args()

    if not args.input.exists():
        print(f"File not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    with open(args.input) as f:
        data = json.load(f)

    # Compile to SVG
    svg_bytes = build_svg(data)
    
    svg_path = args.input.with_suffix(".svg")
    svg_path.write_bytes(svg_bytes)
    print(f"Compiled SVG: {svg_path}")

    # Snapshot to PNG using Playwright
    png_path = args.output or args.input.with_suffix(".png")
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            # Set viewport to diagram dimensions to avoid clipping
            elements = [e for e in data.get("elements", []) if not e.get("isDeleted")]
            min_x, min_y, max_x, max_y = get_bounding_box(elements)
            padding = 50
            width = int(max_x - min_x + padding * 2)
            height = int(max_y - min_y + padding * 2)
            page.set_viewport_size({"width": width, "height": height})
            
            # Load the SVG directly
            page.goto(svg_path.resolve().as_uri())
            # Let rendering complete, then take viewport-based screenshot
            page.wait_for_timeout(1000)
            page.screenshot(path=png_path, omit_background=True)
            browser.close()
        print(f"Rendered PNG: {png_path}")
    except Exception as e:
        print(f"WARNING: PNG snapshot failed (e.g. font timeout): {e}", file=sys.stderr)
        print(f"Vector SVG is still available at: {svg_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
