import * as d3 from 'd3';

export function createRidgelinePlot() {
  // Update margins for better mobile display
  const margin = {
    top: 20,
    right: 10,
    bottom: 10,
    left: 140,
  };
  
  const container = document.getElementById('ridgeline');
  if (!container) return;
  
  // Clear any existing content
  container.innerHTML = '';
  
  const width = container.clientWidth - margin.left - margin.right;
  const height = container.clientHeight - margin.top - margin.bottom;

  // Business metrics
  const metrics = [
    "Daily Active Users",
    "Order Volume",
    "Basket Size",
    "New Customers",
    "Repeat Orders",
    "Revenue per User",
    "Cart Conversions",
    "Payment Success"
  ];

  // Create SVG
  const svg = d3.select('#ridgeline')
    .append('svg')
    .attr('width', '100%')
    .attr('height', '100%')
    .attr('viewBox', `0 0 ${width + margin.left + margin.right} ${height + margin.top + margin.bottom}`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .append('g')
    .attr('transform', `translate(${margin.left},${margin.top})`);

  // Scales
  const x = d3.scaleLinear()
    .domain([0, 100])
    .range([0, width]);

  const y = d3.scalePoint()
    .domain(metrics)
    .range([0, height])
    .padding(0.5);

  // Add y-axis labels
  svg.append('g')
    .attr('class', 'y-axis')
    .call(d3.axisLeft(y)
      .tickSize(0))
    .call(g => g.select('.domain').remove())
    .selectAll('.tick text')
    .attr('class', 'metric-label');

  // Generator factory function with improved smoothing
  function createGenerator(volatility, trend, spikeProb, spikeMagnitude) {
    return function*() {
      let value = 50;
      let momentum = 0;
      const momentumDecay = 0.95;
      while (true) {
        const spike = Math.random() < spikeProb ? (Math.random() * spikeMagnitude + 1.2) : 1;
        momentum = momentum * momentumDecay + (Math.random() - 0.5) * volatility * 10 * spike;
        value += momentum + trend;
        value = Math.max(35, Math.min(65, value));
        yield value;
      }
    };
  }

  // Updated patterns with refined parameters for each metric
  const patterns = {
    "Daily Active Users": createGenerator(0.2, 0.05, 0.05, 1.5),
    "Order Volume": createGenerator(0.25, 0.06, 0.08, 1.8),
    "Basket Size": createGenerator(0.15, 0.02, 0.04, 1.2),
    "New Customers": createGenerator(0.3, 0.08, 0.1, 2.0),
    "Repeat Orders": createGenerator(0.2, 0.04, 0.06, 1.5),
    "Revenue per User": createGenerator(0.18, 0.03, 0.05, 1.4),
    "Cart Conversions": createGenerator(0.15, -0.02, 0.04, 1.3),
    "Payment Success": createGenerator(0.1, 0.01, 0.02, 1.1)
  };

  // Initialize generators
  const generators = {};
  metrics.forEach(metric => {
    generators[metric] = patterns[metric]();
  });

  // Initial data setup
  const pointsPerMetric = 50;
  const metricData = new Map(metrics.map(metric => [
    metric,
    Array.from({ length: pointsPerMetric }, () => generators[metric].next().value)
  ]));

  // Line and area generators with smoother curve
  const line = d3.line()
    .curve(d3.curveBasis)
    .x((d, i) => x(i * (100 / (pointsPerMetric - 1))))
    .y(d => d);

  const area = d3.area()
    .curve(d3.curveBasisClosed)
    .x((d, i) => x(i * (100 / (pointsPerMetric - 1))))
    .y0(d => y(d.metric))
    .y1(d => y(d.metric) + (d.value - 50) * (18 / 50));

  // Create initial paths
  metrics.forEach(metric => {
    const data = metricData.get(metric);
    
    // Add area
    svg.append('path')
      .attr('class', `metric-area ${metric.replace(/\s+/g, '-')}`)
      .datum(data.map(value => ({ metric, value })))
      .attr('d', area);

    // Add line
    svg.append('path')
      .attr('class', `metric-line ${metric.replace(/\s+/g, '-')}`)
      .datum(data.map(value => y(metric) + (value - 50) * (20 / 50)))
      .attr('d', line);
  });

  let frameCount = 0;
  const updateFrequency = 5;

  function animate() {
    frameCount++;

    if (frameCount % updateFrequency === 0) {
      metrics.forEach(metric => {
        const data = metricData.get(metric);
        
        // Shift existing data left
        data.shift();
        // Add new value
        data.push(generators[metric].next().value);

        // Update visualizations
        svg.select(`.metric-area.${metric.replace(/\s+/g, '-')}`)
          .datum(data.map(value => ({ metric, value })))
          .attr('d', area);

        svg.select(`.metric-line.${metric.replace(/\s+/g, '-')}`)
          .datum(data.map(value => y(metric) + (value - 50) * (20 / 50)))
          .attr('d', line);
      });
    }

    requestAnimationFrame(animate);
  }

  animate();
  
  // Return a cleanup function
  return () => {
    container.innerHTML = '';
  };
} 