import React, { useCallback, useEffect, useRef, useState } from "react";

// Zoom, scroll and hover helpers shared by the monitoring charts. A chart
// keeps an x "domain" (the window it shows, in its own x units: time for the
// time series, bin index for the histogram) inside the full data range.

export type XDomain = [number, number];

// Where the plot area sits inside its SVG, in SVG units.
export interface PlotBox {
  width: number;
  height: number;
  left: number;
  top: number;
  plotWidth: number;
  plotHeight: number;
}

// A drag shorter than this (in pixels) is a click, not a zoom.
const MIN_BRUSH_PX = 6;

export const inDomain = (x: number, [x0, x1]: XDomain) => x >= x0 && x <= x1;

// Keep a zoom window inside the data and at least `minSpan` wide.
export const clampDomain = (
  [a, b]: XDomain,
  [lo, hi]: XDomain,
  minSpan: number,
): XDomain => {
  const fullSpan = hi - lo;
  const span = Math.min(Math.max(b - a, Math.min(minSpan, fullSpan)), fullSpan);
  const start = Math.min(Math.max((a + b) / 2 - span / 2, lo), hi - span);
  return [start, start + span];
};

const scaleDomain = ([a, b]: XDomain, factor: number): XDomain => {
  const mid = (a + b) / 2;
  const half = ((b - a) * factor) / 2;
  return [mid - half, mid + half];
};

export const useZoom = (fullDomain: XDomain | null, minSpan: number) => {
  const [view, setView] = useState<XDomain | null>(null);
  const domain = view ?? fullDomain;
  const fullSpan = fullDomain ? fullDomain[1] - fullDomain[0] : 0;
  const span = domain ? domain[1] - domain[0] : 0;

  const zoomTo = (next: XDomain) => {
    if (!fullDomain || fullSpan === 0) return;
    const clamped = clampDomain(next, fullDomain, minSpan);
    // Back at full width means not zoomed.
    setView(clamped[1] - clamped[0] >= fullSpan ? null : clamped);
  };
  const reset = useCallback(() => setView(null), []);

  return {
    domain,
    isZoomed: view !== null,
    canZoomIn: fullSpan > 0 && span > Math.min(minSpan, fullSpan),
    zoomTo,
    zoomIn: () => domain && zoomTo(scaleDomain(domain, 0.5)),
    zoomOut: () => domain && zoomTo(scaleDomain(domain, 2)),
    // Move the window so it starts at `start`, keeping its width.
    panTo: (start: number) => zoomTo([start, start + span]),
    reset,
  };
};

let nextClipId = 0;
export const useClipId = () => useState(() => `chart-clip-${++nextClipId}`)[0];

// Mouse handling on a chart: hover reports the x under the pointer,
// dragging selects a range to zoom into, double-click resets the zoom.
export const usePlotPointer = (
  box: PlotBox,
  domain: XDomain,
  {
    onHover,
    onZoom,
    onReset,
  }: {
    onHover: (x: number | null) => void;
    onZoom: (domain: XDomain) => void;
    onReset: () => void;
  },
) => {
  const svgRef = useRef<SVGSVGElement>(null);
  const [brush, setBrush] = useState<[number, number] | null>(null);
  const [isHovered, setIsHovered] = useState(false);

  const toPx = (clientX: number) => {
    const rect = svgRef.current?.getBoundingClientRect();
    const px =
      rect && rect.width > 0
        ? ((clientX - rect.left) * box.width) / rect.width
        : clientX;
    return Math.min(Math.max(px, box.left), box.left + box.plotWidth);
  };
  const pxToX = (px: number) =>
    domain[0] + ((px - box.left) / box.plotWidth) * (domain[1] - domain[0]);

  const handlers = {
    onMouseDown: (e: React.MouseEvent) => {
      if (e.button !== 0) return;
      e.preventDefault();
      const px = toPx(e.clientX);
      setBrush([px, px]);
    },
    onMouseMove: (e: React.MouseEvent) => {
      const px = toPx(e.clientX);
      setIsHovered(true);
      setBrush((b) => (b ? [b[0], px] : b));
      onHover(pxToX(px));
    },
    onMouseUp: () => {
      if (brush && Math.abs(brush[1] - brush[0]) >= MIN_BRUSH_PX) {
        onZoom([pxToX(Math.min(...brush)), pxToX(Math.max(...brush))]);
      }
      setBrush(null);
    },
    onMouseLeave: () => {
      setBrush(null);
      setIsHovered(false);
      onHover(null);
    },
    onDoubleClick: onReset,
  };

  return { svgRef, handlers, brush, isHovered };
};

// When zoomed in, the chart scrolls sideways through all the data. The
// scroll area is as wide as the whole data range at the current zoom, while
// the chart itself stays pinned in view (axes included) and draws whatever
// window the scroll position points at. Charts that share one domain scroll
// together.
export const ZoomScroller = ({
  box,
  domain,
  fullDomain,
  onPan,
  testId,
  children,
}: {
  box: PlotBox;
  domain: XDomain;
  fullDomain: XDomain;
  onPan: (start: number) => void;
  testId?: string;
  children: React.ReactNode;
}) => {
  const ref = useRef<HTMLDivElement>(null);
  const span = domain[1] - domain[0];
  const pxPerUnit = span > 0 ? box.plotWidth / span : 0;
  const extraWidth = (fullDomain[1] - fullDomain[0] - span) * pxPerUnit;
  const scrollLeft = (domain[0] - fullDomain[0]) * pxPerUnit;

  useEffect(() => {
    const el = ref.current;
    if (el && Math.abs(el.scrollLeft - scrollLeft) > 1) {
      el.scrollLeft = scrollLeft;
    }
  }, [scrollLeft]);

  const onScroll = () => {
    const el = ref.current;
    if (!el || pxPerUnit === 0 || Math.abs(el.scrollLeft - scrollLeft) <= 1) {
      return;
    }
    onPan(fullDomain[0] + el.scrollLeft / pxPerUnit);
  };

  return (
    <div
      ref={ref}
      onScroll={onScroll}
      data-testid={testId}
      style={{
        width: box.width,
        overflowX: extraWidth > 0.5 ? "auto" : "hidden",
      }}
    >
      <div style={{ width: box.width + Math.max(extraWidth, 0) }}>
        <div style={{ position: "sticky", left: 0, width: box.width }}>
          {children}
        </div>
      </div>
    </div>
  );
};

// Hover guide line and drag selection, drawn on top of the plot.
export const PlotOverlay = ({
  box,
  hoverPx,
  brush,
}: {
  box: PlotBox;
  hoverPx: number | null;
  brush: [number, number] | null;
}) => (
  <g pointerEvents="none">
    {hoverPx !== null && (
      <line
        x1={hoverPx}
        y1={box.top}
        x2={hoverPx}
        y2={box.top + box.plotHeight}
        stroke="#98A2B3"
        strokeDasharray="2,2"
      />
    )}
    {brush && (
      <rect
        x={Math.min(...brush)}
        y={box.top}
        width={Math.abs(brush[1] - brush[0])}
        height={box.plotHeight}
        fill="rgba(0, 107, 180, 0.12)"
        stroke="#006BB4"
      />
    )}
  </g>
);

export interface TooltipRow {
  label: string;
  color: string;
  values: string[];
}

export const ChartTooltip = ({
  left,
  box,
  title,
  rows,
}: {
  left: number;
  box: PlotBox;
  title: string;
  rows: TooltipRow[];
}) => {
  const flip = left > box.width - 200;
  return (
    <div
      role="tooltip"
      style={{
        position: "absolute",
        top: box.top,
        left: flip ? undefined : left + 12,
        right: flip ? box.width - left + 12 : undefined,
        background: "#FFFFFF",
        border: "1px solid #D3DAE6",
        borderRadius: 4,
        boxShadow: "0 2px 6px rgba(0, 0, 0, 0.12)",
        padding: "6px 8px",
        fontSize: 12,
        whiteSpace: "nowrap",
        pointerEvents: "none",
        zIndex: 1,
      }}
    >
      <div style={{ fontWeight: 600, marginBottom: 4 }}>{title}</div>
      {rows.map((r) => (
        <div
          key={r.label}
          style={{ display: "flex", alignItems: "center", gap: 6 }}
        >
          <span
            style={{
              width: 8,
              height: 8,
              borderRadius: 2,
              backgroundColor: r.color,
            }}
          />
          <span style={{ color: "#69707D" }}>{r.label}</span>
          <span style={{ marginLeft: "auto", paddingLeft: 8, fontWeight: 600 }}>
            {r.values.join(", ")}
          </span>
        </div>
      ))}
    </div>
  );
};
