/**
 * @module dynatrace
 */

const { Dynatrace, TimeSeries } = require("@dynatrace/api-client");



module.exports.parse = async (dtconfig, spec) => {
  const dynatrace = new Dynatrace(dtconfig);

  async function fetchStats(query) {
    const params = {};

    const percentile = /p\d+/;
    if (query.aggregation.match(percentile)) {
      params.aggregationType = "percentile";
    } else {
      params.aggregationType = query.aggregation;
    }
    params.relativeTime = "day";
    params.entities = query.entityIds;
    params.tags = query.tags;
    params.queryMode = "TOTAL";
    try {
      const timeseries = await dynatrace.timeseries(query.timeseriesId, params);
      // console.log(timeseries);
      // const stats = TimeSeries.stats(timeseries);
      return timeseries;

    } catch (err) {
      console.error(err);
      throw err;
    }
  }


  function parseSources(sourcelist) {
    const sources = {};

    sourcelist.forEach((source) => {
      if (!sources[source.id]) {
        sources[source.id] = {};
      }

      const currentSource = sources[source.id];

      source.metrics.forEach((metric) => {
        currentSource[metric.id] = metric;
      });
    });
    return sources;
  }

  function analyze(source, metric, stats, unit, aggregation) {


    // const query = source.query;
    let hasViolation = false;
    const violations = [];
    const thresholds = metric.thresholds;
    if (!thresholds) return false;

    if (thresholds.lowerSevere && stats[0][1] <= thresholds.lowerSevere) {
      hasViolation = true;
      violations.push({
        id: metric.metricsId,
        name: source.name,
        aggregation,
        value: stats[0][1],
        unit,
        breach: "lower_critical",
        comparison: "fixed",
        threshold: thresholds.lowerSevere,
        score: metric.metricScore,
        raw: stats,
      });
    } else if (thresholds.lowerWarning && stats[0][1] <= thresholds.lowerWarning) {
      hasViolation = true;
      violations.push({
        id: metric.metricsId,
        name: source.name,
        aggregation,
        value: stats[0][1],
        unit,
        breach: "lower_warning",
        comparison: "fixed",
        threshold: thresholds.lowerWarning,
        score: metric.metricScore,
        raw: stats,
      });
    }

    if (thresholds.upperSevere && stats[0][1] >= thresholds.upperSevere) {
      hasViolation = true;
      violations.push({
        id: metric.metricsId,
        name: source.name,
        aggregation,
        value: stats[0][1],
        unit,
        breach: "upper_critical",
        comparison: "fixed",
        threshold: thresholds.upperSevere,
        score: metric.metricScore,
        raw: stats,
      });
    } else if (thresholds.upperWarning && stats[0][1] >= thresholds.upperWarning) {
      hasViolation = true;
      violations.push({
        id: metric.metricsId,
        name: source.name,
        aggregation,
        value: stats[0][1],
        unit,
        breach: "upper_warning",
        comparison: "fixed",
        threshold: thresholds.upperWarning,
        score: metric.metricScore,
        raw: stats,
      });
    }

    if (hasViolation) return violations;
    return false;
  }

  async function runSignature(sources, signature) {
    return signature.map(async (metric) => {
      const sourceDefinition = sources[metric.metricsSourceId][metric.metricsId];
      // Todo: find out how to do percentiles on API
      if (sourceDefinition.query && sourceDefinition.query.timeseriesId && sourceDefinition.query.aggregation != "p90") {
        const stats = await fetchStats(sourceDefinition.query);


        return Object.keys(stats.dataPoints).map((key) => {
          const analyzeResult = analyze(sourceDefinition, metric, stats.dataPoints[key], stats.unit, stats.aggregationType);
          if (analyzeResult) {
            return {
              id: key,
              name: stats.entities[key],
              metrics: analyzeResult
            };
          }
        }).filter(x => x);

      }
    }).filter(x => x);
  }

  try {
    const sources = parseSources(spec.sources);
    const sig = await runSignature(sources, spec.signature);
    const res = await Promise.all(sig);
    const entities = res.filter((elm) => {
      if (elm && !elm.length) return false;
      return elm;
    });
    const result = {
      monspec: "",
      timeframe: 86400,
      tenant_id: dynatrace.baseUrl,
      entities: entities[0] || [],
    };

    return result;
  } catch (err) {
    console.error(err);
    throw err;
  }
};
