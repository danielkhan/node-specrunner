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
    params.tag = query.tags;
    try {
      const timeseries = await dynatrace.timeseries(query.timeseriesId, params);
      const stats = TimeSeries.stats(timeseries);
      return stats;

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

  function analyze(source, metric, stats) {
    // const query = source.query;
    let hasViolation = false;
    const violations = [];
    const thresholds = metric.thresholds;
    if (!thresholds) return false;


    if (thresholds.lowerSevere && stats.min.value <= thresholds.lowerSevere) {
      hasViolation = true;
      violations.push({
        id: metric.metricsId,
        name: source.name,
        aggregation: stats.meta.aggregationType,
        value: stats.min.value,
        unit: stats.meta.unit,
        breach: "lower_critical",
        comparison: "fixed",
        threshold: thresholds.lowerSevere,
        score: metric.metricScore,
        // raw: stats,
      });
    } else if (thresholds.lowerWarning && stats.min.value <= thresholds.lowerWarning) {
      hasViolation = true;
      violations.push({
        id: metric.metricsId,
        name: source.name,
        aggregation: stats.meta.aggregationType,
        value: stats.min.value,
        unit: stats.meta.unit,
        breach: "lower_warning",
        comparison: "fixed",
        threshold: thresholds.lowerWarning,
        score: metric.metricScore,
        // raw: stats,
      });
    }

    if (thresholds.upperSevere && stats.max.value >= thresholds.upperSevere) {
      hasViolation = true;
      violations.push({
        id: metric.metricsId,
        name: source.name,
        aggregation: stats.meta.aggregationType,
        value: stats.max.value,
        unit: stats.meta.unit,
        breach: "upper_critical",
        comparison: "fixed",
        threshold: thresholds.upperSevere,
        score: metric.metricScore,
        // raw: stats,
      });
    } else if (thresholds.upperWarning && stats.max.value >= thresholds.upperWarning) {
      hasViolation = true;
      violations.push({
        id: metric.metricsId,
        name: source.name,
        aggregation: stats.meta.aggregationType,
        value: stats.max.value,
        unit: stats.meta.unit,
        breach: "upper_warning",
        comparison: "fixed",
        threshold: thresholds.upperWarning,
        score: metric.metricScore,
        // raw: stats,
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
        if (stats && stats.constructor === Object && Object.keys(stats).length !== 0) {
          return Object.keys(stats).map((key) => {
            const analyzeResult = analyze(sourceDefinition, metric, stats[key]);
            if (analyzeResult) {
              return {
                id: key,
                metrics: analyzeResult
              };
            }
          }).filter(x => x);
        }
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
