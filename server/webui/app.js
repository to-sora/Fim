const statusEl = document.getElementById("status");
const tableEl = document.getElementById("result-table");
const metaEl = document.getElementById("result-meta");
const graphEl = document.getElementById("graph-output");

const columnsFull = [
  { key: "machine_name", label: "Machine" },
  { key: "file_path", label: "Path" },
  { key: "file_name", label: "File" },
  { key: "size_human", label: "Size" },
  { key: "sha256_count", label: "SHA256 Count" },
  { key: "scan_ts", label: "Scan TS" },
  { key: "ingested_at", label: "Ingested At" },
  { key: "urn", label: "URN" },
  { key: "sha256", label: "SHA256" },
];

const columnsName = [
  { key: "file_name", label: "File" },
  { key: "sha256", label: "SHA256" },
  { key: "scan_ts", label: "Scan TS" },
  { key: "ingested_at", label: "Ingested At" },
];

let currentRecords = [];
let currentColumns = [];
let sortState = { key: null, dir: 1 };
let lastTableQuery = "";
let lastGraphQuery = "";
let lastGraphText = "";

function setStatus(text, isError = false) {
  statusEl.textContent = text;
  statusEl.classList.toggle("error", isError);
}

function clearTable() {
  tableEl.innerHTML = "";
}

function renderTable(records, columns) {
  clearTable();
  if (!records || records.length === 0) {
    metaEl.textContent = "No records.";
    return;
  }

  currentRecords = records.slice();
  currentColumns = columns;

  if (sortState.key) {
    currentRecords = sortRecords(currentRecords, sortState.key, sortState.dir);
  }

  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");
  columns.forEach((col) => {
    const th = document.createElement("th");
    const button = document.createElement("button");
    button.className = "sort-button";
    button.type = "button";
    button.dataset.key = col.key;
    button.textContent = col.label;
    if (sortState.key === col.key) {
      button.textContent += sortState.dir === 1 ? " ▲" : " ▼";
    }
    button.addEventListener("click", () => {
      const nextDir = sortState.key === col.key ? -sortState.dir : 1;
      sortState = { key: col.key, dir: nextDir };
      renderTable(currentRecords, currentColumns);
    });
    th.appendChild(button);
    headRow.appendChild(th);
  });
  thead.appendChild(headRow);
  tableEl.appendChild(thead);

  const tbody = document.createElement("tbody");
  currentRecords.forEach((row) => {
    const tr = document.createElement("tr");
    columns.forEach((col) => {
      const td = document.createElement("td");
      let value = row[col.key];
      if (value === undefined || value === null || value === "") {
        value = "-";
      }
      td.textContent = String(value);
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  tableEl.appendChild(tbody);

  const sortLabel = sortState.key ? ` • sorted by ${sortState.key}` : "";
  metaEl.textContent = `${currentRecords.length} records${sortLabel}`;
}

function csvEscape(value) {
  const str = String(value ?? "");
  if (str.includes('"') || str.includes(",") || str.includes("\n")) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

function formatTimestamp() {
  const now = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  return `${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}_${pad(
    now.getHours()
  )}${pad(now.getMinutes())}${pad(now.getSeconds())}`;
}

function downloadFile(content, filename, type) {
  const blob = new Blob([content], { type });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

async function copyToClipboard(text) {
  if (navigator.clipboard && window.isSecureContext) {
    await navigator.clipboard.writeText(text);
    return;
  }
  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.style.position = "fixed";
  textarea.style.opacity = "0";
  document.body.appendChild(textarea);
  textarea.focus();
  textarea.select();
  const ok = document.execCommand("copy");
  textarea.remove();
  if (!ok) {
    throw new Error("clipboard copy blocked by browser");
  }
}

function getTableColumns() {
  if (currentColumns && currentColumns.length) {
    return currentColumns;
  }
  if (currentRecords.length) {
    return Object.keys(currentRecords[0]).map((key) => ({ key, label: key }));
  }
  return [];
}

function buildCsvPayload() {
  const cols = getTableColumns();
  if (!cols.length) {
    return null;
  }
  const header = cols.map((c) => csvEscape(c.label)).join(",");
  const rows = currentRecords.map((row) =>
    cols.map((c) => csvEscape(row[c.key] ?? "")).join(",")
  );
  rows.push(`# query: ${lastTableQuery}`);
  return [header, ...rows].join("\n");
}

function sortRecords(records, key, dir) {
  const numericKeys = new Set(["size_bytes", "sha256_count"]);
  const dateKeys = new Set(["scan_ts", "ingested_at"]);
  const sizeAlias = key === "size_human";
  const sortKey = sizeAlias ? "size_bytes" : key;

  return records.slice().sort((a, b) => {
    let va = a[sortKey];
    let vb = b[sortKey];

    if (va === undefined || va === null || va === "") va = null;
    if (vb === undefined || vb === null || vb === "") vb = null;
    if (va === null && vb === null) return 0;
    if (va === null) return 1;
    if (vb === null) return -1;

    if (numericKeys.has(sortKey)) {
      return (Number(va) - Number(vb)) * dir;
    }
    if (dateKeys.has(sortKey)) {
      return (new Date(va).getTime() - new Date(vb).getTime()) * dir;
    }
    return String(va).localeCompare(String(vb)) * dir;
  });
}

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `HTTP ${res.status}`);
  }
  return res.json();
}

async function loadMachines() {
  try {
    const payload = await fetchJson("/api/machines");
    const list = document.getElementById("machine-list");
    list.innerHTML = "";
    payload.machines.forEach((name) => {
      const option = document.createElement("option");
      option.value = name;
      list.appendChild(option);
    });
  } catch (err) {
    setStatus(`Failed to load machines: ${err.message}`, true);
  }
}

function getValue(id) {
  return document.getElementById(id).value.trim();
}

function getChecked(id) {
  return document.getElementById(id).checked;
}

document.getElementById("btn-machine").addEventListener("click", async () => {
  const machine = getValue("machine-name");
  const sha = getValue("machine-sha");
  const limit = getValue("machine-limit");
  const dedupe = getChecked("machine-dedupe");
  if (!machine) {
    setStatus("Machine name is required.", true);
    return;
  }
  setStatus("Querying machine...");
  graphEl.textContent = "No graph yet.";
  try {
    const params = new URLSearchParams({
      machine_name: machine,
      limit: limit || "0",
      dedupe: dedupe ? "1" : "0",
    });
    if (sha) {
      params.set("sha256", sha);
    }
    const payload = await fetchJson(`/api/query/machine?${params.toString()}`);
    renderTable(payload.records || [], columnsFull);
    lastTableQuery = `machine_name=${machine} sha256=${sha || ""} limit=${limit || "0"} dedupe=${
      dedupe ? "1" : "0"
    }`;
    setStatus("Machine query done.");
  } catch (err) {
    setStatus(`Machine query failed: ${err.message}`, true);
  }
});

document.getElementById("btn-sha256").addEventListener("click", async () => {
  const sha = getValue("sha256-value");
  const limit = getValue("sha256-limit");
  const dedupe = getChecked("sha256-dedupe");
  if (!sha) {
    setStatus("SHA256 is required.", true);
    return;
  }
  setStatus("Querying sha256...");
  graphEl.textContent = "No graph yet.";
  try {
    const params = new URLSearchParams({
      sha256: sha,
      limit: limit || "100",
      dedupe: dedupe ? "1" : "0",
    });
    const payload = await fetchJson(`/api/query/file?${params.toString()}`);
    renderTable(payload.records || [], columnsFull);
    lastTableQuery = `sha256=${sha} limit=${limit || "100"} dedupe=${dedupe ? "1" : "0"}`;
    setStatus("SHA256 query done.");
  } catch (err) {
    setStatus(`SHA256 query failed: ${err.message}`, true);
  }
});

document.getElementById("btn-name").addEventListener("click", async () => {
  const substring = getValue("name-substring");
  const machine = getValue("name-machine");
  const limit = getValue("name-limit");
  if (!substring) {
    setStatus("Substring is required.", true);
    return;
  }
  setStatus("Searching names...");
  graphEl.textContent = "No graph yet.";
  try {
    const params = new URLSearchParams({
      substring,
      limit: limit || "0",
    });
    if (machine) {
      params.set("machine_name", machine);
    }
    const payload = await fetchJson(`/api/query/name?${params.toString()}`);
    renderTable(payload.records || [], columnsName);
    lastTableQuery = `substring=${substring} machine_name=${machine || ""} limit=${limit || "0"}`;
    setStatus("Name search done.");
  } catch (err) {
    setStatus(`Name search failed: ${err.message}`, true);
  }
});

document.getElementById("btn-graph").addEventListener("click", async () => {
  const sha = getValue("graph-sha");
  const fmt = document.getElementById("graph-format").value;
  const limit = getValue("graph-limit");
  if (!sha) {
    setStatus("SHA256 is required.", true);
    return;
  }
  setStatus("Rendering graph...");
  try {
    const params = new URLSearchParams({
      sha256: sha,
      fmt,
      limit: limit || "20000",
    });
    const res = await fetch(`/api/graph/sha256?${params.toString()}`);
    if (!res.ok) {
      const text = await res.text();
      throw new Error(text || `HTTP ${res.status}`);
    }
    const text = await res.text();
    lastGraphQuery = `sha256=${sha} fmt=${fmt} limit=${limit || "20000"}`;
    lastGraphText = text || "(empty)";
    graphEl.textContent = lastGraphText;
    setStatus("Graph rendered.");
  } catch (err) {
    setStatus(`Graph failed: ${err.message}`, true);
  }
});

document.getElementById("btn-table-csv").addEventListener("click", () => {
  if (!currentRecords.length) {
    setStatus("No table data to export.", true);
    return;
  }
  const csv = buildCsvPayload();
  if (!csv) {
    setStatus("No table columns to export.", true);
    return;
  }
  const filename = `fim-table-${formatTimestamp()}.csv`;
  downloadFile(csv, filename, "text/csv");
  setStatus("CSV exported.");
});

document.getElementById("btn-table-copy").addEventListener("click", async () => {
  if (!currentRecords.length) {
    setStatus("No table data to copy.", true);
    return;
  }
  const csv = buildCsvPayload();
  if (!csv) {
    setStatus("No table columns to copy.", true);
    return;
  }
  try {
    await copyToClipboard(csv);
    setStatus("Table copied to clipboard.");
  } catch (err) {
    setStatus(`Copy failed: ${err.message}`, true);
  }
});

document.getElementById("btn-graph-copy").addEventListener("click", async () => {
  if (!lastGraphText) {
    setStatus("No graph data to copy.", true);
    return;
  }
  const payload = `# query: ${lastGraphQuery}\n${lastGraphText}`;
  try {
    await copyToClipboard(payload);
    setStatus("Graph copied to clipboard.");
  } catch (err) {
    setStatus(`Graph copy failed: ${err.message}`, true);
  }
});

document.getElementById("btn-graph-download").addEventListener("click", () => {
  if (!lastGraphText) {
    setStatus("No graph data to download.", true);
    return;
  }
  const payload = `# query: ${lastGraphQuery}\n${lastGraphText}`;
  const filename = `fim-graph-${formatTimestamp()}.txt`;
  downloadFile(payload, filename, "text/plain");
  setStatus("Graph downloaded.");
});

loadMachines();
