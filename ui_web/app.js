(function () {
  const state = {
    bridge: null,
    bootstrap: null,
    screenData: null,
    group: "monitoring",
    section: "overview",
    selectedSourceId: null,
    selectedJobId: null,
    sourceSearch: "",
    sourceCategory: "",
    selectedRows: {},
    filters: {
      content: { query: "" },
      search: { query: "" },
      claims: { query: "", status: "" },
      cases: { query: "" },
      entities: { query: "", entity_type: "" },
      relations: { query: "", layer: "", view: "cards" },
      officials: { query: "", active_only: true, view: "cards" },
      settings: {},
    },
    collapsedSourceGroups: {},
    collapsedTaskGroups: {},
    tasksCollapsed: false,
    taskTab: "queue",
  };

  const ui = {};
  let sourceReloadTimer = null;
  let screenReloadTimer = null;

  document.addEventListener("DOMContentLoaded", init);

  async function init() {
    cacheUi();
    bindShellEvents();
    await initBridge();
    await loadBootstrap();
  }

  function cacheUi() {
    ui.appShell = document.getElementById("app-shell");
    ui.sourceGroups = document.getElementById("source-groups");
    ui.sourceSearchInput = document.getElementById("source-search-input");
    ui.navGroupRow = document.getElementById("nav-group-row");
    ui.navSectionRow = document.getElementById("nav-section-row");
    ui.breadcrumbLine = document.getElementById("breadcrumb-line");
    ui.summaryStrip = document.getElementById("summary-strip");
    ui.screenRoot = document.getElementById("screen-root");
    ui.screenEyebrow = document.getElementById("screen-eyebrow");
    ui.screenTitle = document.getElementById("screen-title");
    ui.screenCaption = document.getElementById("screen-caption");
    ui.tasksGroups = document.getElementById("tasks-groups");
    ui.jobDetailCard = document.getElementById("job-detail-card");
    ui.toastStack = document.getElementById("toast-stack");
    ui.toggleTasksBtn = document.getElementById("toggle-tasks-btn");
    ui.schedulerToggleBtn = document.getElementById("scheduler-toggle-btn");
  }

  async function initBridge() {
    if (window.qt && window.qt.webChannelTransport && typeof window.QWebChannel === "function") {
      state.bridge = await new Promise((resolve) => {
        new window.QWebChannel(window.qt.webChannelTransport, (channel) => {
          resolve(channel.objects.dashboardBridge);
        });
      });

      if (state.bridge.bootstrapChanged && typeof state.bridge.bootstrapChanged.connect === "function") {
        state.bridge.bootstrapChanged.connect(async () => {
          await loadBootstrap();
        });
      }
      if (state.bridge.toastRaised && typeof state.bridge.toastRaised.connect === "function") {
        state.bridge.toastRaised.connect((payload) => {
          const toast = parsePayload(payload);
          if (toast && toast.message) {
            raiseToast(toast.message, toast.level || "info");
          }
        });
      }
      return;
    }

    state.bridge = createMockBridge();
  }

  async function loadBootstrap() {
    const payload = await bridgeCall("getBootstrap");
    if (!payload) {
      return;
    }
    state.bootstrap = payload;

    const availableGroup = payload.navigation.find((group) => group.key === state.group) || payload.navigation[0];
    state.group = availableGroup.key;
    if (!availableGroup.sections.find((section) => section.key === state.section)) {
      state.section = availableGroup.sections[0].key;
    }

    if (!state.selectedJobId && payload.jobs.items.length) {
      state.selectedJobId = payload.jobs.items[0].id;
    }

    renderShell();
    await loadCurrentScreen();
  }

  async function loadCurrentScreen() {
    const payload = await bridgeCall(
      "getScreenPayload",
      JSON.stringify({
        screen: state.section,
        filters: buildScreenFilters(),
      })
    );
    state.screenData = payload || { items: [], detail: null };
    renderScreen();
  }

  function buildScreenFilters() {
    const base = { ...(state.filters[state.section] || {}) };
    const selectedId = state.selectedRows[state.section];
    if (selectedId) {
      base.selected_id = selectedId;
    }
    if ((state.section === "content" || state.section === "search") && state.selectedSourceId) {
      base.source_id = state.selectedSourceId;
    }
    return base;
  }

  function bindShellEvents() {
    document.getElementById("manual-refresh-btn").addEventListener("click", manualRefresh);
    document.getElementById("refresh-sources-btn").addEventListener("click", requestSources);
    document.getElementById("export-obsidian-btn").addEventListener("click", () => {
      bridgeVoid("exportObsidian");
    });

    ui.toggleTasksBtn.addEventListener("click", () => {
      state.tasksCollapsed = !state.tasksCollapsed;
      renderShell();
    });

    ui.schedulerToggleBtn.addEventListener("click", () => {
      bridgeVoid("toggleScheduler");
      setTimeout(manualRefresh, 200);
    });

    ui.sourceSearchInput.addEventListener("input", (event) => {
      state.sourceSearch = event.target.value;
      scheduleSourcesReload();
    });

    document.querySelectorAll("[data-source-category]").forEach((button) => {
      button.addEventListener("click", () => {
        state.sourceCategory = button.dataset.sourceCategory || "";
        document.querySelectorAll("[data-source-category]").forEach((chip) => {
          chip.classList.toggle("active", chip === button);
        });
        requestSources();
      });
    });

    setupResize(document.getElementById("sidebar-resize"), "--sidebar-width", 240, 420);
    setupResize(document.getElementById("tasks-resize"), "--tasks-width", 300, 480);
  }

  function setupResize(handle, variableName, min, max) {
    if (!handle) {
      return;
    }
    handle.addEventListener("mousedown", (event) => {
      event.preventDefault();
      const startX = event.clientX;
      const startValue = parseInt(
        getComputedStyle(document.documentElement).getPropertyValue(variableName),
        10
      );
      const isLeft = handle.id === "sidebar-resize";
      const onMove = (moveEvent) => {
        const delta = moveEvent.clientX - startX;
        const nextValue = isLeft ? startValue + delta : startValue - delta;
        const clamped = Math.max(min, Math.min(max, nextValue));
        document.documentElement.style.setProperty(variableName, `${clamped}px`);
      };
      const onUp = () => {
        window.removeEventListener("mousemove", onMove);
        window.removeEventListener("mouseup", onUp);
      };
      window.addEventListener("mousemove", onMove);
      window.addEventListener("mouseup", onUp);
    });
  }

  function renderShell() {
    renderNavigation();
    renderBreadcrumbs();
    renderSummary();
    renderSources(state.bootstrap.sources);
    renderJobs(state.bootstrap.jobs);

    ui.appShell.classList.toggle("tasks-collapsed", state.tasksCollapsed);
    ui.appShell.dataset.group = state.group;
    ui.appShell.dataset.section = state.section;
    ui.toggleTasksBtn.textContent = state.tasksCollapsed ? "Показать панель" : "Панель";
    ui.schedulerToggleBtn.textContent = state.bootstrap.jobs.scheduler_running
      ? "Стоп план"
      : "Планировщик";
  }

  function renderNavigation() {
    ui.navGroupRow.innerHTML = state.bootstrap.navigation
      .map(
        (group) => `
          <button class="nav-pill ${group.key === state.group ? "active" : ""}" data-nav-group="${group.key}">
            ${escapeHtml(group.label)}
          </button>
        `
      )
      .join("");

    const currentGroup = state.bootstrap.navigation.find((group) => group.key === state.group);
    ui.navSectionRow.innerHTML = (currentGroup?.sections || [])
      .map(
        (section) => `
          <button class="section-pill ${section.key === state.section ? "active" : ""}" data-nav-section="${section.key}">
            ${escapeHtml(section.label)}
          </button>
        `
      )
      .join("");

    ui.navGroupRow.querySelectorAll("[data-nav-group]").forEach((button) => {
      button.addEventListener("click", async () => {
        state.group = button.dataset.navGroup;
        const nextGroup = state.bootstrap.navigation.find((group) => group.key === state.group);
        state.section = nextGroup.sections[0].key;
        renderShell();
        await loadCurrentScreen();
      });
    });

    ui.navSectionRow.querySelectorAll("[data-nav-section]").forEach((button) => {
      button.addEventListener("click", async () => {
        state.section = button.dataset.navSection;
        renderNavigation();
        renderBreadcrumbs();
        updateScreenHeading();
        await loadCurrentScreen();
      });
    });
    updateScreenHeading();
  }

  function updateScreenHeading() {
    const currentGroup = state.bootstrap.navigation.find((group) => group.key === state.group);
    const currentSection = currentGroup.sections.find((section) => section.key === state.section);
    ui.screenEyebrow.textContent = currentGroup.label.toUpperCase();
    ui.screenTitle.textContent = currentSection.label;
    ui.screenCaption.textContent = screenCaption(state.section);
  }

  function renderBreadcrumbs() {
    const currentGroup = state.bootstrap.navigation.find((group) => group.key === state.group);
    const currentSection = currentGroup.sections.find((section) => section.key === state.section);
    ui.breadcrumbLine.textContent = `${currentSourceName()} / ${currentGroup.label} / ${currentSection.label}`;
  }

  function renderSummary() {
    const counts = state.bootstrap.summary.counts;
    const cards = [
      ["Контент", counts.content, `${state.bootstrap.summary.running_jobs.length} jobs running`, "content"],
      ["Заявления", counts.claims, state.bootstrap.summary.scheduler_running ? "Scheduler ON" : "Scheduler OFF", "claims"],
      ["Сущности", counts.entities, "Entity knowledge graph", "entities"],
      ["Дела", counts.cases, "Case layer", "cases"],
      ["Руководство", counts.officials, "Directory layer", "officials"],
      ["Связи", counts.relations, "Evidence + structural", "relations"],
    ];
    ui.summaryStrip.innerHTML = cards
      .map(
        ([label, value, meta, kind], index) => `
          <article class="summary-card" data-metric="${escapeHtml(kind)}" style="animation-delay:${index * 28}ms">
            <div class="label">${escapeHtml(label)}</div>
            <div class="value">${escapeHtml(String(value))}</div>
            <div class="meta">${escapeHtml(meta)}</div>
            <div class="metric-bar"></div>
          </article>
        `
      )
      .join("");
  }

  function renderSources(payload) {
    const groups = payload.groups || [];
    ui.sourceGroups.innerHTML = groups
      .map((group) => {
        const collapsed = !!state.collapsedSourceGroups[group.key];
        return `
          <section class="source-group">
            <button class="group-toggle" data-source-group="${group.key}">
              <span>${escapeHtml(group.label)}</span>
              <span class="toggle-glyph">${collapsed ? "+" : "−"}</span>
            </button>
            ${
              collapsed
                ? ""
                : `<div class="source-list">
                    ${group.items
                      .map(
                        (item) => `
                          <article class="source-row ${state.selectedSourceId === item.id ? "active" : ""}" data-source-id="${item.id}">
                            <div>
                              <div class="source-name">${escapeHtml(item.name)}</div>
                              <div class="source-meta">${escapeHtml(item.category || "")} · tier ${escapeHtml(
                                item.credibility_tier || "—"
                              )}</div>
                            </div>
                            <button class="pin-btn ${item.pinned ? "active" : ""}" data-pin-source="${item.id}" type="button">
                              ${item.pinned ? "★" : "☆"}
                            </button>
                          </article>
                        `
                      )
                      .join("")}
                   </div>`
            }
          </section>
        `;
      })
      .join("");

    ui.sourceGroups.querySelectorAll("[data-source-group]").forEach((button) => {
      button.addEventListener("click", () => {
        const key = button.dataset.sourceGroup;
        state.collapsedSourceGroups[key] = !state.collapsedSourceGroups[key];
        renderSources(payload);
      });
    });

    ui.sourceGroups.querySelectorAll("[data-source-id]").forEach((row) => {
      row.addEventListener("click", async (event) => {
        if (event.target.closest("[data-pin-source]")) {
          return;
        }
        state.selectedSourceId = Number(row.dataset.sourceId);
        renderSources(payload);
        renderBreadcrumbs();
        if (state.section === "content" || state.section === "search") {
          await loadCurrentScreen();
        }
      });
    });

    ui.sourceGroups.querySelectorAll("[data-pin-source]").forEach((button) => {
      button.addEventListener("click", async (event) => {
        event.stopPropagation();
        const sourceId = Number(button.dataset.pinSource);
        await bridgeCall("togglePinSource", sourceId);
        await requestSources();
      });
    });
  }

  function renderJobs(payload) {
    const groups = payload.groups || [];
    const logs = payload.logs || [];
    const selected = payload.items.find((item) => item.id === state.selectedJobId) || payload.items[0];
    if (selected) {
      state.selectedJobId = selected.id;
    }

    const queueMarkup = groups.length
      ? groups.map((group) => {
          const groupKey = group.key || group.label || "tasks";
          const collapsed = !!state.collapsedTaskGroups[groupKey];
          const items = group.items || [];
          const runningCount = items.filter((item) => item.running).length;
          return `
            <section class="task-group">
              <button class="group-toggle task-group-toggle" data-task-group="${escapeHtml(groupKey)}" type="button">
                <span>${escapeHtml(group.label || groupKey)}</span>
                <span class="group-meta"><span class="status-dot ${runningCount ? "status-running" : "status-idle"}"></span>${runningCount}/${items.length} <span class="toggle-glyph">${collapsed ? "+" : "−"}</span></span>
              </button>
              ${
                collapsed
                  ? ""
                  : `<div class="job-list">
                      ${items.map((item) => `
                        <article class="job-chip ${state.selectedJobId === item.id ? "active" : ""}" data-job-id="${escapeHtml(item.id)}">
                          <div class="job-row-head">
                            <div class="job-primary">${escapeHtml(item.name)}</div>
                            ${renderJobState(item, { compact: true })}
                          </div>
                          <div class="job-secondary">${escapeHtml(item.group)} · ${escapeHtml(humanInterval(item.interval))}</div>
                        </article>
                      `).join("")}
                    </div>`
              }
            </section>
          `;
        }).join("")
      : emptyState("Нет задач", "Registry пока пуст.");

    const logsMarkup = logs.length
      ? logs.slice(-14).reverse().map((logEntry) => `<div class="log-item"><span class="status-dot status-idle"></span>${escapeHtml(logEntry.message || "")}</div>`).join("")
      : '<div class="log-item"><span class="status-dot status-idle"></span>Пока нет сообщений.</div>';

    const settingsMarkup = selected
      ? `
          <div class="detail-grid compact-detail-grid">
            <div class="detail-kv"><div class="k">ID</div><div class="v">${escapeHtml(selected.id)}</div></div>
            <div class="detail-kv"><div class="k">Интервал</div><div class="v">${escapeHtml(humanInterval(selected.interval))}</div></div>
          </div>
          <div class="detail-section compact-detail-section">
            <h3>Изменить интервал</h3>
            <input class="interval-input" id="job-interval-input-panel" type="number" min="30" step="30" value="${escapeHtml(String(selected.interval))}">
          </div>
        `
      : emptyState("Нет задач", "Registry пока пуст.");

    ui.tasksGroups.innerHTML = `
      <div class="task-panel-tabs">
        <button class="task-tab ${state.taskTab === "queue" ? "active" : ""}" data-task-tab="queue" type="button">Очередь</button>
        <button class="task-tab ${state.taskTab === "logs" ? "active" : ""}" data-task-tab="logs" type="button">Логи</button>
        <button class="task-tab ${state.taskTab === "settings" ? "active" : ""}" data-task-tab="settings" type="button">Параметры</button>
      </div>
      <div class="task-page">
        ${state.taskTab === "queue" ? queueMarkup : state.taskTab === "logs" ? `<div class="log-list">${logsMarkup}</div>` : settingsMarkup}
      </div>
    `;

    ui.tasksGroups.querySelectorAll("[data-task-tab]").forEach((button) => {
      button.addEventListener("click", () => {
        state.taskTab = button.dataset.taskTab;
        renderJobs(payload);
      });
    });

    ui.tasksGroups.querySelectorAll("[data-task-group]").forEach((button) => {
      button.addEventListener("click", () => {
        const key = button.dataset.taskGroup;
        state.collapsedTaskGroups[key] = !state.collapsedTaskGroups[key];
        renderJobs(payload);
      });
    });

    ui.tasksGroups.querySelectorAll("[data-job-id]").forEach((chip) => {
      chip.addEventListener("click", () => {
        state.selectedJobId = chip.dataset.jobId;
        renderJobs(payload);
      });
    });

    const panelIntervalInput = document.getElementById("job-interval-input-panel");
    if (panelIntervalInput && selected) {
      panelIntervalInput.addEventListener("change", (event) => {
        const nextValue = Math.max(30, Number(event.target.value || selected.interval));
        bridgeVoid("updateJobInterval", selected.id, nextValue);
        setTimeout(manualRefresh, 200);
      });
    }

    if (!selected) {
      ui.jobDetailCard.innerHTML = emptyState("Нет задач", "Registry пока пуст.");
      return;
    }

    ui.jobDetailCard.innerHTML = `
      <div class="job-detail-head">
        <div>
          <div class="eyebrow">Task Detail</div>
          <h3 class="detail-title">${escapeHtml(selected.name)}</h3>
        </div>
        ${renderJobState(selected)}
      </div>
      <div class="detail-grid compact-detail-grid">
        <div class="detail-kv"><div class="k">Группа</div><div class="v">${escapeHtml(selected.group)}</div></div>
        <div class="detail-kv"><div class="k">Интервал</div><div class="v">${escapeHtml(humanInterval(selected.interval))}</div></div>
      </div>
      <div class="job-actions">
        <button class="run-btn" id="run-job-btn" type="button"${selected.running ? " disabled" : ""}>Запустить</button>
        <button class="stop-btn" id="stop-job-btn" type="button"${selected.running ? "" : " disabled"}>Стоп</button>
      </div>
    `;

    document.getElementById("run-job-btn").addEventListener("click", () => {
      bridgeVoid("runJob", selected.id);
      setTimeout(manualRefresh, 300);
    });
    document.getElementById("stop-job-btn").addEventListener("click", () => {
      bridgeVoid("stopJob", selected.id);
      setTimeout(manualRefresh, 300);
    });
  }

  function jobStatusClass(item) {
    if (!item) return "status-error";
    if (item.running) return "status-running";
    if (Number(item.interval || 0) <= 0) return "status-error";
    return "status-idle";
  }

  function jobStatusText(item) {
    if (!item) return "Нет";
    if (item.running) return "Выполняется";
    if (Number(item.interval || 0) <= 0) return "Отключено";
    return "Ожидание";
  }

  function renderJobState(item, options = {}) {
    const compact = !!options.compact;
    const statusClass = jobStatusClass(item);
    const title = escapeHtml(jobStatusText(item));
    return `
      <span class="job-state ${statusClass} ${compact ? "compact" : ""}" title="${title}">
        <span class="status-dot ${statusClass}"></span>
        ${compact ? "" : title}
      </span>
    `;
  }

  function renderScreen() {
    if (!state.screenData) {
      return;
    }
    ui.appShell.dataset.group = state.group;
    ui.appShell.dataset.section = state.section;
    ui.screenRoot.classList.remove("screen-root-enter");
    void ui.screenRoot.offsetWidth;
    ui.screenRoot.classList.add("screen-root-enter");
    switch (state.section) {
      case "overview":
        renderOverviewScreen(state.screenData);
        break;
      case "content":
      case "search":
        renderContentScreen(state.section, state.screenData);
        break;
      case "claims":
        renderClaimsScreen(state.screenData);
        break;
      case "cases":
        renderCasesScreen(state.screenData);
        break;
      case "entities":
        renderEntitiesScreen(state.screenData);
        break;
      case "relations":
        renderRelationsScreen(state.screenData);
        break;
      case "officials":
        renderOfficialsScreen(state.screenData);
        break;
      case "settings":
        renderSettingsScreen(state.screenData);
        break;
      default:
        ui.screenRoot.innerHTML = emptyState("Экран не найден", "Выберите другой раздел.");
    }
  }

  function renderOverviewScreen(payload) {
    const recentContent = payload.recent_content || [];
    const recentCases = payload.recent_cases || [];
    const counts = payload.secondary_counts || {};
    const graphHealth = payload.graph_health || {};
    const runtimeHealth = payload.runtime_health || {};
    const lowAccountability = payload.low_accountability || [];
    const countOrder = [
      "persons",
      "quotes",
      "flagged_quotes",
      "tags",
      "sources",
      "deputies",
      "evidence",
      "attachments",
      "bills",
      "votes",
      "investigation_materials",
    ];
    const countEntries = countOrder
      .filter((key) => typeof counts[key] === "number" || /^\d+$/.test(String(counts[key] ?? "")))
      .map((key) => [key, counts[key]]);
    const metricsMarkup = countEntries.length
      ? countEntries
          .map(([key, value], index) => `
            <div class="system-tile metric-tile" style="animation-delay:${index * 18}ms">
              <div class="label">${escapeHtml(countLabel(key))}</div>
              <strong>${escapeHtml(String(value))}</strong>
              <div class="meta">${escapeHtml(countMeta(key))}</div>
            </div>
          `)
          .join("")
      : `
          <div class="system-tile metric-tile"><div class="label">Источники</div><strong>0</strong><div class="meta">операционный слой</div></div>
          <div class="system-tile metric-tile"><div class="label">Evidence</div><strong>0</strong><div class="meta">граф пуст</div></div>
        `;

    const lowAccountabilityMarkup = lowAccountability.length
      ? lowAccountability
          .map(
            (item) => `
              <article class="mini-item">
                <div class="table-row-head">
                  <div class="table-primary">${escapeHtml(item.full_name || "—")}</div>
                  <span class="badge rose">score ${escapeHtml(Number(item.calculated_score || 0).toFixed(1))}</span>
                </div>
                <div class="table-secondary">${escapeHtml(item.faction || "—")} · выступления ${escapeHtml(
                  String(item.public_speeches_count || 0)
                )} · флаги ${escapeHtml(String(item.flagged_statements_count || 0))} · дела ${escapeHtml(
                  String(item.linked_cases_count || 0)
                )}</div>
              </article>
            `
          )
          .join("")
      : emptyState("Нет индекса подотчётности", "Сначала пересчитайте accountability layer.");

    const analysisState =
      graphHealth.pipeline_version && graphHealth.analysis_pipeline_version
        ? graphHealth.pipeline_version === graphHealth.analysis_pipeline_version
          ? "sync"
          : "lag"
        : "unknown";
    const exportState =
      graphHealth.pipeline_version && graphHealth.export_pipeline_version
        ? graphHealth.pipeline_version === graphHealth.export_pipeline_version
          ? "sync"
          : "lag"
        : "unknown";

    const healthMarkup = [
      ["Evidence-backed", graphHealth.evidence_backed_relations, "связи с evidence item"],
      ["Weak backlog", graphHealth.weak_relations, "pending relation candidates"],
      ["Promoted weak", graphHealth.promoted_candidates, "promoted candidate edges"],
      ["Tagged items", graphHealth.tagged_items, "материалы с тегами"],
      ["Untagged", graphHealth.untagged_items, "ещё без tag pipeline"],
      ["Granular pending", graphHealth.granular_pending, "ещё без granular stage"],
      ["Daemon", runtimeHealth.daemon_running ? "ON" : "OFF", "фоновый runtime"],
      ["Running jobs", runtimeHealth.running_jobs, "активные lease jobs"],
      ["Degraded sources", graphHealth.degraded_sources ?? runtimeHealth.degraded_sources, "источники в degraded state"],
      ["Dead letters", graphHealth.dead_letters ?? runtimeHealth.dead_letters, "необработанные сбои ingest/media"],
      ["Analysis", graphHealth.analysis_pipeline_version || "—", `build ${analysisState}`],
      ["Export", graphHealth.export_pipeline_version || "—", `vault ${exportState}`],
    ]
      .filter(([, value]) => value !== undefined)
      .map(
        ([label, value, meta]) => `
          <div class="detail-kv compact-kpi">
            <div class="k">${escapeHtml(label)}</div>
            <div class="v">${escapeHtml(String(value ?? 0))}</div>
            <div class="muted">${escapeHtml(meta)}</div>
          </div>
        `
      )
      .join("");

    ui.screenRoot.innerHTML = `
      <div class="overview-grid overview-grid-dense">
        <div class="overview-column">
          <section class="overview-panel metrics-panel scrollable">
            <h3>Операционный срез</h3>
            <div class="system-grid dense-metrics-grid">
              ${metricsMarkup}
            </div>
          </section>

          <section class="overview-panel scrollable">
            <h3>Слабая подотчётность</h3>
            <div class="mini-list">
              ${lowAccountabilityMarkup}
            </div>
          </section>
        </div>

        <div class="overview-column">
          <section class="overview-panel command-panel">
            <div class="overview-hero compact-hero">
              <div class="eyebrow">Command Center</div>
              <h3 class="hero-title">Оперативная карта ingest, графа и tag pipeline</h3>
              <p class="hero-copy">Обзор больше не повторяет верхние карточки: здесь только то, что помогает понять качество графа, покрытие тегов и текущий operational state.</p>
            </div>
            <div class="detail-grid health-grid">
              ${healthMarkup}
            </div>
          </section>

          <section class="overview-panel scrollable">
            <h3>Последний контент</h3>
            <div class="mini-list">
              ${
                recentContent.length
                  ? recentContent
                      .map(
                        (item) => `
                          <article class="mini-item">
                            <div class="table-row-head">
                              <div class="table-primary">${escapeHtml(item.title || "Без названия")}</div>
                              <span class="badge cyan">signal</span>
                            </div>
                            <div class="table-secondary">${escapeHtml(item.source_name || "Источник")} · ${escapeHtml(formatDate(item.published_at))}</div>
                          </article>
                        `
                      )
                      .join("")
                  : emptyState("Пусто", "Контент пока не найден.")
              }
            </div>
          </section>

          <section class="overview-panel scrollable">
            <h3>Последние дела</h3>
            <div class="mini-list">
              ${
                recentCases.length
                  ? recentCases
                      .map(
                        (item) => `
                          <article class="mini-item">
                            <div class="table-row-head">
                              <div class="table-primary">${escapeHtml(item.title || "Без названия")}</div>
                              <span class="badge emerald">case</span>
                            </div>
                            <div class="table-secondary">${escapeHtml(item.case_type || "—")} · claims ${escapeHtml(String(item.claims_count || 0))}</div>
                          </article>
                        `
                      )
                      .join("")
                  : emptyState("Пусто", "Cases пока не собраны.")
              }
            </div>
          </section>
        </div>
      </div>
    `;
  }

  function countLabel(key) {
    const labels = {
      content: "Контент",
      claims: "Заявления",
      entities: "Сущности",
      cases: "Дела",
      persons: "Персоны",
      quotes: "Цитаты",
      flagged_quotes: "Флаговые цитаты",
      tags: "Теги",
      sources: "Источники",
      officials: "Руководство",
      deputies: "Депутаты",
      evidence: "Evidence",
      attachments: "Вложения",
      bills: "Законопроекты",
      votes: "Голосования",
      investigation_materials: "Следств. мат.",
      relations: "Связи",
    };
    return labels[key] || key.replace(/_/g, " ");
  }

  function countMeta(key) {
    const labels = {
      content: "посты, статьи, видео",
      claims: "извлечённые claims",
      entities: "персоны, организации, места",
      cases: "связанные кейсы",
      persons: "NER и профили",
      quotes: "прямые и косвенные",
      flagged_quotes: "риторические маркеры",
      tags: "уникальные метки",
      sources: "активные каналы",
      officials: "directory profiles",
      deputies: "активные профили",
      evidence: "claim/evidence links",
      attachments: "файлы и изображения",
      bills: "собранные bills",
      votes: "сессии голосований",
      investigation_materials: "расследования, суды",
      relations: "entity_relations",
    };
    return labels[key] || "метрика базы";
  }

  function renderContentScreen(section, payload) {
    const query = state.filters[section].query || "";
    ui.screenRoot.innerHTML = renderMasterDetailLayout({
      filters: `
        <div class="screen-filters">
          <input id="screen-query-input" class="glass-input" type="search" placeholder="${
            section === "search" ? "Искать по контенту..." : "Фильтр по контенту..."
          }" value="${escapeHtml(query)}">
        </div>
      `,
      list: renderTableList(
        payload.items,
        (item) => `
          <div class="table-row-head">
            <div class="table-primary">${escapeHtml(item.title || "Без названия")}</div>
            <span class="badge ${item.status === "raw_signal" ? "amber" : "emerald"}">${escapeHtml(
              item.status || "—"
            )}</span>
          </div>
          <div class="table-secondary">${escapeHtml(item.source_name || "Источник")} · ${escapeHtml(
            formatDate(item.published_at)
          )}</div>
        `
      ),
      detail: payload.detail
        ? `
            <h3 class="detail-title">${escapeHtml(payload.detail.title || "Без названия")}</h3>
            <div class="detail-grid">
              <div class="detail-kv"><div class="k">Источник</div><div class="v">${escapeHtml(
                payload.detail.source_name || "—"
              )}</div></div>
              <div class="detail-kv"><div class="k">Дата</div><div class="v">${escapeHtml(
                formatDate(payload.detail.published_at)
              )}</div></div>
            </div>
            <div class="detail-section"><h3>Текст</h3><div class="muted">${escapeHtml(
              payload.detail.body_text || "Нет текста"
            )}</div></div>
            ${renderLinkSection("Сущности", payload.detail.entities, (entity) => `${escapeHtml(entity.canonical_name)} · ${escapeHtml(entity.mention_type)}`)}
            ${renderLinkSection("Claims", payload.detail.claims, (claim) => escapeHtml(claim.claim_text))}
          `
        : emptyState("Нет выбранного объекта", "Выберите запись слева."),
    });
    bindTextFilter(section);
    bindRowSelection(section);
  }

  function renderClaimsScreen(payload) {
    const query = state.filters.claims.query || "";
    const status = state.filters.claims.status || "";
    ui.screenRoot.innerHTML = renderMasterDetailLayout({
      filters: `
        <div class="screen-filters">
          <input id="screen-query-input" class="glass-input" type="search" placeholder="Поиск по claims" value="${escapeHtml(query)}">
          <div class="status-filter-row">
            ${["", "verified", "unverified", "disputed"].map(
              (value) => `
                <button class="filter-chip ${status === value ? "active" : ""}" data-claim-status="${value}">
                  ${value || "Все"}
                </button>
              `
            ).join("")}
          </div>
        </div>
      `,
      list: renderTableList(
        payload.items,
        (item) => `
          <div class="table-row-head">
            <div class="table-primary">${escapeHtml(truncate(item.claim_text, 120))}</div>
            <span class="badge ${badgeClass(item.status)}">${escapeHtml(item.status || "—")}</span>
          </div>
          <div class="table-secondary">${escapeHtml(item.content_title || "Контент")} ${item.case_id ? `· case #${escapeHtml(String(item.case_id))}` : ""}</div>
        `
      ),
      detail: payload.detail
        ? `
            <h3 class="detail-title">${escapeHtml(payload.detail.claim_text || "Claim")}</h3>
            <div class="detail-grid">
              <div class="detail-kv"><div class="k">Статус</div><div class="v">${escapeHtml(payload.detail.status || "—")}</div></div>
              <div class="detail-kv"><div class="k">Контент</div><div class="v">${escapeHtml(payload.detail.content_title || "—")}</div></div>
            </div>
            ${renderLinkSection("Evidence", payload.detail.evidence, (item) => `${escapeHtml(item.evidence_type || "evidence")} · ${escapeHtml(item.evidence_title || "—")}`)}
          `
        : emptyState("Нет выбранного claim", "Выберите запись слева."),
    });
    bindTextFilter("claims");
    ui.screenRoot.querySelectorAll("[data-claim-status]").forEach((button) => {
      button.addEventListener("click", async () => {
        state.filters.claims.status = button.dataset.claimStatus;
        await loadCurrentScreen();
      });
    });
    bindRowSelection("claims");
  }

  function renderCasesScreen(payload) {
    const query = state.filters.cases.query || "";
    ui.screenRoot.innerHTML = renderMasterDetailLayout({
      filters: `
        <div class="screen-filters">
          <input id="screen-query-input" class="glass-input" type="search" placeholder="Фильтр по делам" value="${escapeHtml(query)}">
        </div>
      `,
      list: renderTableList(
        payload.items,
        (item) => `
          <div class="table-row-head">
            <div class="table-primary">${escapeHtml(item.title || "Без названия")}</div>
            <span class="badge ${badgeClass(item.status)}">${escapeHtml(item.status || "—")}</span>
          </div>
          <div class="table-secondary">${escapeHtml(item.case_type || "—")} · ${escapeHtml(
            String(item.claims_count || 0)
          )} claims</div>
        `
      ),
      detail: payload.detail
        ? `
            <h3 class="detail-title">${escapeHtml(payload.detail.title || "Case")}</h3>
            <div class="detail-grid">
              <div class="detail-kv"><div class="k">Тип</div><div class="v">${escapeHtml(payload.detail.case_type || "—")}</div></div>
              <div class="detail-kv"><div class="k">Статус</div><div class="v">${escapeHtml(payload.detail.status || "—")}</div></div>
            </div>
            ${renderLinkSection("Claims", payload.detail.claims, (item) => escapeHtml(item.claim_text))}
            ${renderLinkSection("Events", payload.detail.events, (item) => `${escapeHtml(formatDate(item.event_date))} · ${escapeHtml(item.event_title || "—")}`)}
          `
        : emptyState("Нет выбранного дела", "Выберите case слева."),
    });
    bindTextFilter("cases");
    bindRowSelection("cases");
  }

  function renderEntitiesScreen(payload) {
    const query = state.filters.entities.query || "";
    ui.screenRoot.innerHTML = renderMasterDetailLayout({
      filters: `
        <div class="screen-filters">
          <input id="screen-query-input" class="glass-input" type="search" placeholder="Поиск по сущностям" value="${escapeHtml(query)}">
        </div>
      `,
      list: renderTableList(
        payload.items,
        (item) => `
          <div class="table-row-head">
            <div class="table-primary">${escapeHtml(item.canonical_name || "Entity")}</div>
            <span class="badge">${escapeHtml(item.entity_type || "—")}</span>
          </div>
          <div class="table-secondary">${escapeHtml(String(item.content_count || 0))} content · ${escapeHtml(
            String(item.positions_count || 0)
          )} positions</div>
        `
      ),
      detail: payload.detail ? renderEntityDetail(payload.detail) : emptyState("Нет выбранной сущности", "Выберите entity слева."),
    });
    bindTextFilter("entities");
    bindRowSelection("entities");
  }

  function renderRelationsScreen(payload) {
    const query = state.filters.relations.query || "";
    const layer = state.filters.relations.layer || "";
    const view = state.filters.relations.view || "cards";
    ui.screenRoot.innerHTML = renderMasterDetailLayout({
      filters: `
        <div class="screen-filters">
          <input id="screen-query-input" class="glass-input" type="search" placeholder="Поиск по связям" value="${escapeHtml(query)}">
          <div class="layer-filter-row">
            ${["", "structural", "evidence", "weak_similarity"].map(
              (value) => `
                <button class="filter-chip ${layer === value ? "active" : ""}" data-relation-layer="${value}">
                  ${value || "Все"}
                </button>
              `
            ).join("")}
          </div>
          ${renderViewSwitch("relations", view)}
        </div>
      `,
      list:
        view === "table"
          ? renderDataTable({
              items: payload.items,
              columns: ["Откуда", "Тип", "Куда", "Слой", "Сила"],
              rowId: (item) => item.id,
              rowCells: (item) => [
                escapeHtml(item.from_name || "—"),
                escapeHtml(item.relation_type || "—"),
                escapeHtml(item.to_name || "—"),
                `<span class="badge ${item.layer === "structural" ? "emerald" : item.layer === "weak_similarity" ? "amber" : "cyan"}">${escapeHtml(item.layer || "—")}</span>`,
                escapeHtml(item.strength || "—"),
              ],
            })
          : renderTableList(
              payload.items,
              (item) => `
                <div class="table-row-head">
                  <div class="table-primary">${escapeHtml(item.from_name)} → ${escapeHtml(item.to_name)}</div>
                  <span class="badge ${item.layer === "structural" ? "emerald" : item.layer === "weak_similarity" ? "amber" : "cyan"}">
                    ${escapeHtml(item.layer)}
                  </span>
                </div>
                <div class="table-secondary">${escapeHtml(item.relation_type)} · ${escapeHtml(item.detected_by || "—")}</div>
              `
            ),
      detail: payload.detail
        ? `
            <h3 class="detail-title">${escapeHtml(payload.detail.from_name)} → ${escapeHtml(payload.detail.to_name)}</h3>
            <div class="detail-grid">
              <div class="detail-kv"><div class="k">Тип</div><div class="v">${escapeHtml(payload.detail.relation_type)}</div></div>
              <div class="detail-kv"><div class="k">Layer</div><div class="v">${escapeHtml(payload.detail.layer)}</div></div>
              <div class="detail-kv"><div class="k">Strength</div><div class="v">${escapeHtml(payload.detail.strength || "—")}</div></div>
              <div class="detail-kv"><div class="k">Detected by</div><div class="v">${escapeHtml(payload.detail.detected_by || "—")}</div></div>
            </div>
          `
        : emptyState("Нет выбранной связи", "Выберите relation слева."),
    });
    bindTextFilter("relations");
    ui.screenRoot.querySelectorAll("[data-relation-layer]").forEach((button) => {
      button.addEventListener("click", async () => {
        state.filters.relations.layer = button.dataset.relationLayer;
        await loadCurrentScreen();
      });
    });
    bindViewSwitch("relations");
    bindRowSelection("relations");
  }

  function renderOfficialsScreen(payload) {
    const query = state.filters.officials.query || "";
    const view = state.filters.officials.view || "cards";
    ui.screenRoot.innerHTML = renderMasterDetailLayout({
      filters: `
        <div class="screen-filters">
          <input id="screen-query-input" class="glass-input" type="search" placeholder="Поиск по руководству" value="${escapeHtml(query)}">
          ${renderViewSwitch("officials", view)}
        </div>
      `,
      list:
        view === "table"
          ? renderDataTable({
              items: payload.items,
              columns: ["ФИО", "Орган", "Должность", "Evidence"],
              rowId: (item) => item.entity_id,
              rowCells: (item) => [
                escapeHtml(item.full_name || "—"),
                escapeHtml(item.organization || "—"),
                escapeHtml(item.position_title || "—"),
                escapeHtml(String(item.content_count || 0)),
              ],
            })
          : renderTableList(
              payload.items,
              (item) => `
                <div class="table-row-head">
                  <div class="table-primary">${escapeHtml(item.full_name || "—")}</div>
                  <span class="badge emerald">${escapeHtml(item.position_title || "—")}</span>
                </div>
                <div class="table-secondary">${escapeHtml(item.organization || "—")} · ${escapeHtml(
                  String(item.content_count || 0)
                )} evidence items</div>
              `
            ),
      detail: payload.detail ? renderEntityDetail(payload.detail) : emptyState("Нет выбранного профиля", "Выберите чиновника слева."),
    });
    bindTextFilter("officials");
    bindViewSwitch("officials");
    bindRowSelection("officials", "entity_id");
  }

  function renderSettingsScreen(payload) {
    const items = payload.items || [];
    ui.screenRoot.innerHTML = `
      <div class="settings-grid">
        ${
          items.length
            ? items
                .map(
                  (item) => `
                    <div class="detail-kv">
                      <div class="k">${escapeHtml(item.key)}</div>
                      <div class="v">${escapeHtml(String(item.value ?? "—"))}</div>
                    </div>
                  `
                )
                .join("")
            : emptyState("Нет публичных настроек", "Настройки появятся здесь, когда backend их отдаст.")
        }
      </div>
    `;
  }

  function renderEntityDetail(detail) {
    return `
      <h3 class="detail-title">${escapeHtml(detail.canonical_name || detail.full_name || "Entity")}</h3>
      <div class="detail-grid">
        <div class="detail-kv"><div class="k">Тип</div><div class="v">${escapeHtml(detail.entity_type || "—")}</div></div>
        <div class="detail-kv"><div class="k">ИНН</div><div class="v">${escapeHtml(detail.inn || "—")}</div></div>
      </div>
      ${
        detail.description
          ? `<div class="detail-section"><h3>Описание</h3><div class="muted">${escapeHtml(detail.description)}</div></div>`
          : ""
      }
      ${renderLinkSection("Должности", detail.positions, (item) => `${escapeHtml(item.position_title || "—")} · ${escapeHtml(item.organization || "—")}`)}
      ${renderLinkSection("Связанный контент", detail.content, (item) => `${escapeHtml(formatDate(item.published_at))} · ${escapeHtml(item.title || "—")}`)}
      ${renderLinkSection("Claims", detail.claims, (item) => escapeHtml(item.claim_text || "—"))}
      ${renderLinkSection("Cases", detail.cases, (item) => escapeHtml(item.title || "—"))}
      ${renderLinkSection("Связи", detail.relations, (item) => `${escapeHtml(item.relation_type || "—")} · ${escapeHtml(
        item.from_name || ""
      )} → ${escapeHtml(item.to_name || "")}`)}
    `;
  }

  function renderMasterDetailLayout({ filters, list, detail }) {
    return `
      <div class="master-detail">
        <section class="master-pane">
          ${filters || ""}
          <div class="table-list">${list}</div>
        </section>
        <section class="detail-pane">${detail}</section>
      </div>
    `;
  }

  function renderViewSwitch(section, currentView) {
    return `
      <div class="view-switch" data-view-switch="${escapeHtml(section)}">
        <button class="view-switch-btn ${currentView === "cards" ? "active" : ""}" data-screen-view="${escapeHtml(section)}:cards" type="button">Карточки</button>
        <button class="view-switch-btn ${currentView === "table" ? "active" : ""}" data-screen-view="${escapeHtml(section)}:table" type="button">Таблица</button>
      </div>
    `;
  }

  function renderTableList(items, rowRenderer) {
    if (!items || !items.length) {
      return emptyState("Пусто", "Нет данных под текущий фильтр.");
    }
    return items
      .map((item) => {
        const rowId =
          item.id ?? item.entity_id ?? item.content_id ?? item.case_id ?? item.claim_id;
        const selectedValue = state.selectedRows[state.section];
        const isActive =
          String(selectedValue || "") === String(rowId || "") ||
          (!selectedValue && String(state.screenData?.detail?.id ?? state.screenData?.detail?.entity_id ?? "") === String(rowId || ""));
        return `
          <article class="table-row ${isActive ? "active" : ""}" data-row-id="${escapeHtml(String(rowId))}">
            ${rowRenderer(item)}
          </article>
        `;
      })
      .join("");
  }

  function renderDataTable({ items, columns, rowId, rowCells }) {
    if (!items || !items.length) {
      return emptyState("Пусто", "Нет данных под текущий фильтр.");
    }
    const selectedValue = state.selectedRows[state.section];
    const rows = items
      .map((item) => {
        const currentRowId = rowId(item);
        const isActive =
          String(selectedValue || "") === String(currentRowId || "") ||
          (!selectedValue &&
            String(state.screenData?.detail?.id ?? state.screenData?.detail?.entity_id ?? "") ===
              String(currentRowId || ""));
        return `
          <tr class="data-table-row ${isActive ? "active" : ""}" data-row-id="${escapeHtml(String(currentRowId))}">
            ${rowCells(item).map((cell) => `<td>${cell}</td>`).join("")}
          </tr>
        `;
      })
      .join("");
    return `
      <div class="data-table-wrap">
        <table class="data-table">
          <thead>
            <tr>${columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("")}</tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    `;
  }

  function renderLinkSection(title, items, formatter) {
    if (!items || !items.length) {
      return "";
    }
    return `
      <div class="detail-section">
        <h3>${escapeHtml(title)}</h3>
        <ul>
          ${items.map((item) => `<li>${formatter(item)}</li>`).join("")}
        </ul>
      </div>
    `;
  }

  function bindTextFilter(section) {
    const input = document.getElementById("screen-query-input");
    if (!input) {
      return;
    }
    input.addEventListener("input", (event) => {
      state.filters[section].query = event.target.value;
      scheduleScreenReload();
    });
  }

  function bindRowSelection(section, idField) {
    ui.screenRoot.querySelectorAll("[data-row-id]").forEach((row) => {
      row.addEventListener("click", async () => {
        state.selectedRows[section] = Number(row.dataset.rowId);
        if (idField === "entity_id") {
          state.selectedRows[section] = Number(row.dataset.rowId);
        }
        await loadCurrentScreen();
      });
    });
  }

  function bindViewSwitch(section) {
    ui.screenRoot.querySelectorAll("[data-screen-view]").forEach((button) => {
      button.addEventListener("click", async () => {
        const [targetSection, nextView] = String(button.dataset.screenView || "").split(":");
        if (targetSection !== section || !nextView) {
          return;
        }
        state.filters[section].view = nextView;
        await loadCurrentScreen();
      });
    });
  }

  async function requestSources() {
    const payload = await bridgeCall(
      "getSources",
      JSON.stringify({ search: state.sourceSearch, category: state.sourceCategory })
    );
    if (!payload) {
      return;
    }
    state.bootstrap.sources = payload;
    renderSources(payload);
    renderBreadcrumbs();
  }

  function scheduleSourcesReload() {
    clearTimeout(sourceReloadTimer);
    sourceReloadTimer = setTimeout(requestSources, 180);
  }

  function scheduleScreenReload() {
    clearTimeout(screenReloadTimer);
    screenReloadTimer = setTimeout(loadCurrentScreen, 180);
  }

  async function manualRefresh() {
    await loadBootstrap();
  }

  function currentSourceName() {
    if (!state.selectedSourceId || !state.bootstrap?.sources?.groups) {
      return "Все источники";
    }
    for (const group of state.bootstrap.sources.groups) {
      const found = group.items.find((item) => item.id === state.selectedSourceId);
      if (found) {
        return found.name;
      }
    }
    return "Все источники";
  }

  function screenCaption(section) {
    const captions = {
      overview: "Операционный срез, качество графа, тегов и последние изменения по базе.",
      content: "Master-detail по контенту без перегруженных таблиц.",
      search: "Быстрый поиск по контенту и связанным объектам.",
      claims: "Список заявлений, статусов и evidence linkages.",
      cases: "Открытые и собранные дела с таймлайном claims.",
      entities: "Сущности, должности, claims, content и связи.",
      relations: "Структурные, evidence и weak-similarity связи с card/table режимом.",
      officials: "Руководители и заместители госорганов из официальных directory sources с card/table режимом.",
      settings: "Ключевые публичные настройки текущего workspace.",
    };
    return captions[section] || "Данные текущего раздела.";
  }

  function humanInterval(seconds) {
    const value = Number(seconds || 0);
    if (value < 60) {
      return `${value} сек`;
    }
    if (value < 3600) {
      return `${Math.round(value / 60)} мин`;
    }
    if (value < 86400) {
      return `${Math.round(value / 3600)} ч`;
    }
    return `${Math.round(value / 86400)} д`;
  }

  function formatDate(value) {
    if (!value) {
      return "—";
    }
    return String(value).slice(0, 10);
  }

  function truncate(value, limit) {
    const stringValue = String(value || "");
    if (stringValue.length <= limit) {
      return stringValue;
    }
    return `${stringValue.slice(0, limit - 1)}…`;
  }

  function badgeClass(value) {
    const normalized = String(value || "").toLowerCase();
    if (normalized.includes("verif") || normalized.includes("open") || normalized.includes("active")) {
      return "emerald";
    }
    if (normalized.includes("disput") || normalized.includes("error")) {
      return "rose";
    }
    return "amber";
  }

  function emptyState(title, subtitle) {
    return `
      <div class="empty-state">
        <div>
          <div class="table-primary">${escapeHtml(title)}</div>
          <div class="table-secondary">${escapeHtml(subtitle)}</div>
        </div>
      </div>
    `;
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function parsePayload(value) {
    if (typeof value !== "string") {
      return value;
    }
    try {
      return JSON.parse(value);
    } catch (error) {
      return null;
    }
  }

  function bridgeCall(method, ...args) {
    return new Promise((resolve) => {
      if (!state.bridge || typeof state.bridge[method] !== "function") {
        resolve(null);
        return;
      }
      state.bridge[method](...args, (result) => {
        resolve(parsePayload(result));
      });
    });
  }

  function bridgeVoid(method, ...args) {
    if (state.bridge && typeof state.bridge[method] === "function") {
      state.bridge[method](...args);
    }
  }

  function raiseToast(message, level) {
    const node = document.createElement("div");
    node.className = `toast ${level || "info"}`;
    node.textContent = message;
    ui.toastStack.appendChild(node);
    setTimeout(() => {
      node.remove();
    }, 4200);
  }

  function makeSignal() {
    const listeners = [];
    return {
      connect(listener) {
        listeners.push(listener);
      },
      emit(payload) {
        listeners.forEach((listener) => listener(payload));
      },
    };
  }

  function createMockBridge() {
    const bootstrapChanged = makeSignal();
    const toastRaised = makeSignal();
    const mockState = {
      pinnedSources: [1],
      schedulerRunning: false,
      runningJobs: ["executive_directory"],
      sourceGroups: [
        {
          key: "pinned",
          label: "Закреплённые",
          items: [
            { id: 1, name: "Минфин России — руководство", category: "official", credibility_tier: "A", pinned: true },
          ],
        },
        {
          key: "official",
          label: "Официальные",
          items: [
            { id: 2, name: "Роскомнадзор — руководство", category: "official", credibility_tier: "A", pinned: false },
            { id: 3, name: "ФНС России — руководство", category: "official", credibility_tier: "A", pinned: false },
          ],
        },
      ],
    };

    function bootstrap() {
      return {
        navigation: [
          { key: "monitoring", label: "Мониторинг", sections: [{ key: "overview", label: "Обзор" }, { key: "content", label: "Контент" }, { key: "search", label: "Поиск" }] },
          { key: "verification", label: "Проверка", sections: [{ key: "claims", label: "Заявления" }, { key: "cases", label: "Дела" }] },
          { key: "analytics", label: "Аналитика", sections: [{ key: "entities", label: "Сущности" }, { key: "relations", label: "Связи" }, { key: "officials", label: "Руководство" }] },
          { key: "system", label: "Система", sections: [{ key: "settings", label: "Настройки" }] },
        ],
        summary: {
          counts: { content: 128, claims: 46, entities: 302, cases: 12, officials: 52, relations: 814 },
          running_jobs: mockState.runningJobs,
          scheduler_running: mockState.schedulerRunning,
          recent_content: [
            { id: 1, title: "Иванов Иван Иванович — Министр тестирования", source_name: "Минфин", published_at: "2026-04-25" },
            { id: 2, title: "Петров Пётр Петрович — Заместитель руководителя", source_name: "РКН", published_at: "2026-04-24" },
          ],
          recent_cases: [
            { id: 1, title: "Кейс назначения", case_type: "oversight", claims_count: 3 },
            { id: 2, title: "Кейс контракта", case_type: "contract", claims_count: 2 },
          ],
        },
        sources: { groups: mockState.sourceGroups },
        jobs: {
          scheduler_running: mockState.schedulerRunning,
          items: [
            { id: "watch_folder", name: "Inbox-сканер", group: "Сбор", interval: 60, running: false },
            { id: "executive_directory", name: "Руководство органов", group: "Сбор", interval: 604800, running: true },
            { id: "claims", name: "Заявления/верификация", group: "Анализ", interval: 21600, running: false },
          ],
          groups: [
            { label: "Сбор", items: [{ id: "watch_folder", name: "Inbox-сканер", group: "Сбор", interval: 60, running: false }, { id: "executive_directory", name: "Руководство органов", group: "Сбор", interval: 604800, running: true }] },
            { label: "Анализ", items: [{ id: "claims", name: "Заявления/верификация", group: "Анализ", interval: 21600, running: false }] },
          ],
          logs: [{ message: "Mock bridge active" }],
        },
      };
    }

    function screen(screenKey) {
      const screens = {
        overview: bootstrap().summary,
        content: {
          items: [{ id: 11, title: "Executive profile snapshot", status: "raw_signal", source_name: "Минфин", published_at: "2026-04-25" }],
          detail: { id: 11, title: "Executive profile snapshot", source_name: "Минфин", published_at: "2026-04-25", body_text: "Mock content body", entities: [{ canonical_name: "Иванов Иван Иванович", mention_type: "subject" }], claims: [{ claim_text: "Иванов занимает должность" }] },
        },
        search: {
          items: [{ id: 11, title: "Executive profile snapshot", status: "raw_signal", source_name: "Минфин", published_at: "2026-04-25" }],
          detail: { id: 11, title: "Executive profile snapshot", source_name: "Минфин", published_at: "2026-04-25", body_text: "Mock content body", entities: [{ canonical_name: "Иванов Иван Иванович", mention_type: "subject" }], claims: [{ claim_text: "Иванов занимает должность" }] },
        },
        claims: {
          items: [{ id: 21, claim_text: "Иванов Иван Иванович занимает должность министра", status: "verified", content_title: "Executive profile snapshot", case_id: 1 }],
          detail: { id: 21, claim_text: "Иванов Иван Иванович занимает должность министра", status: "verified", content_title: "Executive profile snapshot", evidence: [{ evidence_type: "official_profile", evidence_title: "Executive profile snapshot" }] },
        },
        cases: {
          items: [{ id: 31, title: "Кейс назначения", status: "open", case_type: "oversight", claims_count: 3 }],
          detail: { id: 31, title: "Кейс назначения", status: "open", case_type: "oversight", claims: [{ claim_text: "Иванов занимает должность" }], events: [{ event_date: "2026-04-25", event_title: "Публикация профиля" }] },
        },
        entities: {
          items: [{ id: 1, canonical_name: "Иванов Иван Иванович", entity_type: "person", content_count: 4, positions_count: 1 }],
          detail: { id: 1, entity_id: 1, canonical_name: "Иванов Иван Иванович", entity_type: "person", description: "Министр тестирования", positions: [{ position_title: "Министр тестирования", organization: "Министерство тестирования" }], content: [{ title: "Executive profile snapshot", published_at: "2026-04-25" }], claims: [{ claim_text: "Иванов занимает должность" }], cases: [{ title: "Кейс назначения" }], relations: [{ relation_type: "works_at", from_name: "Иванов Иван Иванович", to_name: "Министерство тестирования" }] },
        },
        relations: {
          items: [{ id: 41, from_name: "Иванов Иван Иванович", to_name: "Министерство тестирования", relation_type: "works_at", layer: "structural", strength: "strong", detected_by: "official_positions" }],
          detail: { id: 41, from_name: "Иванов Иван Иванович", to_name: "Министерство тестирования", relation_type: "works_at", layer: "structural", strength: "strong", detected_by: "official_positions" },
        },
        officials: {
          items: [{ entity_id: 1, full_name: "Иванов Иван Иванович", position_title: "Министр тестирования", organization: "Министерство тестирования", content_count: 4 }],
          detail: { id: 1, entity_id: 1, canonical_name: "Иванов Иван Иванович", entity_type: "person", description: "Министр тестирования", positions: [{ position_title: "Министр тестирования", organization: "Министерство тестирования" }], content: [{ id: 11, title: "Executive profile snapshot", published_at: "2026-04-25" }], claims: [{ claim_text: "Иванов занимает должность" }], cases: [{ title: "Кейс назначения" }], relations: [{ relation_type: "works_at", from_name: "Иванов Иван Иванович", to_name: "Министерство тестирования" }] },
        },
        settings: {
          items: [{ key: "db_path", value: "db/news_unified.db" }, { key: "executive_directory_interval_seconds", value: 604800 }],
          detail: { project_root: "F:/новости" },
        },
      };
      return screens[screenKey] || { items: [], detail: null };
    }

    return {
      bootstrapChanged,
      toastRaised,
      getBootstrap(callback) {
        callback(JSON.stringify(bootstrap()));
      },
      getScreenPayload(payload, callback) {
        const parsed = parsePayload(payload) || {};
        callback(JSON.stringify(screen(parsed.screen || "overview")));
      },
      getSources(payload, callback) {
        callback(JSON.stringify({ groups: mockState.sourceGroups }));
      },
      getJobs(callback) {
        callback(JSON.stringify(bootstrap().jobs));
      },
      togglePinSource(sourceId, callback) {
        callback(JSON.stringify({ ok: true }));
      },
      runJob(jobId) {
        toastRaised.emit(JSON.stringify({ message: `Mock run: ${jobId}`, level: "success" }));
      },
      stopJob(jobId) {
        toastRaised.emit(JSON.stringify({ message: `Mock stop: ${jobId}`, level: "warning" }));
      },
      toggleScheduler() {
        mockState.schedulerRunning = !mockState.schedulerRunning;
        bootstrapChanged.emit(JSON.stringify(bootstrap()));
      },
      updateJobInterval(jobId, seconds) {
        toastRaised.emit(JSON.stringify({ message: `Mock interval ${jobId}: ${seconds}`, level: "info" }));
      },
      exportObsidian() {
        toastRaised.emit(JSON.stringify({ message: "Mock Obsidian export", level: "success" }));
      },
    };
  }

  window.__dashboardApp = {
    manualRefresh,
  };
})();
