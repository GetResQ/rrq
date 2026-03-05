import fs from "node:fs";
import path from "node:path";

type FileSummary = {
  total: number;
  covered: number;
};

function normalizePath(inputPath: string, rootDir: string): string {
  const absolute = path.isAbsolute(inputPath)
    ? inputPath
    : path.resolve(rootDir, inputPath);
  return path.relative(rootDir, absolute).replaceAll("\\", "/");
}

function parseLcov(content: string): Map<string, FileSummary> {
  const files = new Map<string, FileSummary>();
  let currentFile: string | null = null;
  const lines = content.split(/\r?\n/);
  for (const line of lines) {
    if (line.startsWith("SF:")) {
      currentFile = line.slice(3);
      files.set(currentFile, { total: 0, covered: 0 });
      continue;
    }
    if (currentFile === null) {
      continue;
    }
    const summary = files.get(currentFile);
    if (!summary) {
      continue;
    }
    if (line.startsWith("LF:")) {
      const total = Number.parseInt(line.slice(3), 10);
      if (Number.isFinite(total)) {
        summary.total = total;
      }
      continue;
    }
    if (line.startsWith("LH:")) {
      const covered = Number.parseInt(line.slice(3), 10);
      if (Number.isFinite(covered)) {
        summary.covered = covered;
      }
    }
  }
  return files;
}

function summarizeCoverage(files: Map<string, FileSummary>): {
  byFile: Map<string, FileSummary>;
  total: FileSummary;
} {
  const byFile = new Map(files);
  let totalLines = 0;
  let coveredLines = 0;
  for (const summary of files.values()) {
    totalLines += summary.total;
    coveredLines += summary.covered;
  }
  return {
    byFile,
    total: {
      total: totalLines,
      covered: coveredLines,
    },
  };
}

function main(): never {
  const [lcovPathArg, thresholdArg, ...expectedFilesArg] = process.argv.slice(2);
  if (!lcovPathArg || !thresholdArg) {
    console.error(
      "Usage: bun scripts/check-coverage.ts <lcov-path> <threshold> [expected-files...]",
    );
    process.exit(2);
  }

  const threshold = Number.parseFloat(thresholdArg);
  if (!Number.isFinite(threshold) || threshold < 0 || threshold > 100) {
    console.error(`Invalid threshold: ${thresholdArg}`);
    process.exit(2);
  }

  const rootDir = process.cwd();
  if (!fs.existsSync(lcovPathArg)) {
    console.error(`Coverage file not found: ${lcovPathArg}`);
    process.exit(1);
  }

  const parsedFiles = parseLcov(fs.readFileSync(lcovPathArg, "utf-8"));
  const normalizedFiles = new Map<string, FileSummary>();
  for (const [filePath, summary] of parsedFiles.entries()) {
    normalizedFiles.set(normalizePath(filePath, rootDir), summary);
  }

  if (normalizedFiles.size === 0) {
    console.error("No files were found in lcov coverage output.");
    process.exit(1);
  }

  const missingExpected = expectedFilesArg
    .map((file) => normalizePath(file, rootDir))
    .filter((file) => !normalizedFiles.has(file));
  if (missingExpected.length > 0) {
    console.error(
      `Missing expected files in coverage report: ${missingExpected.join(", ")}`,
    );
    process.exit(1);
  }

  const { total } = summarizeCoverage(normalizedFiles);
  const percent = total.total === 0 ? 100 : (total.covered / total.total) * 100;
  console.log(
    `TypeScript line coverage: ${percent.toFixed(2)}% (${total.covered}/${total.total})`,
  );
  if (percent < threshold) {
    console.error(
      `Coverage ${percent.toFixed(2)}% is below threshold ${threshold.toFixed(2)}%.`,
    );
    process.exit(1);
  }
  process.exit(0);
}

main();
