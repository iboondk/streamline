// Summaries over real samples only. Nothing here invents a value: every output is
// derived from the samples passed in, and fewer than two samples is an error.

// Nearest-rank percentile on a sorted ascending array: the value at rank ceil(p/100 * n).
function percentile(sortedAsc, p) {
  const rank = Math.ceil((p / 100) * sortedAsc.length);
  const index = Math.min(sortedAsc.length - 1, Math.max(0, rank - 1));
  return sortedAsc[index];
}

export function summarize(samples) {
  if (!Array.isArray(samples)) {
    throw new TypeError('samples must be an array of { t, rows, lag }');
  }
  if (samples.length < 2) {
    throw new Error(`need at least 2 samples to summarize, got ${samples.length}`);
  }

  for (const s of samples) {
    for (const key of ['t', 'rows', 'lag']) {
      if (typeof s[key] !== 'number' || !Number.isFinite(s[key])) {
        throw new TypeError(`sample field '${key}' must be a finite number: ${JSON.stringify(s)}`);
      }
    }
  }

  const ordered = [...samples].sort((a, b) => a.t - b.t);
  const first = ordered[0];
  const last = ordered[ordered.length - 1];

  const durationSec = (last.t - first.t) / 1000;
  if (!(durationSec > 0)) {
    throw new Error('samples span no time; cannot compute a rate');
  }

  const rowsDelta = last.rows - first.rows;
  const lags = ordered.map((s) => s.lag).sort((a, b) => a - b);

  return {
    durationSec,
    rowsDelta,
    rowsPerSec: rowsDelta / durationSec,
    lagMax: lags[lags.length - 1],
    lagP50: percentile(lags, 50),
    lagP95: percentile(lags, 95),
    lagFinal: last.lag,
  };
}
