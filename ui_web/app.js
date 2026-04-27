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
      events: { query: "" },
      review_ops: { query: "", queue: "", status: "open" },
      entities: { query: "", entity_type: "" },
      relations: { query: "", layer: "", view: "cards", map_group: "" },
      officials: { query: "", active_only: true, view: "cards" },
      settings: {},
    },
    collapsedSourceGroups: {},
    collapsedTaskGroups: {},
    detailDrawerOpen: {},
    sidebarCollapsed: true,
    tasksCollapsed: true,
    taskTab: "queue",
  };

  const ui = {};
  const GRAPH_WIDTH = 1000;
  const GRAPH_HEIGHT = 620;
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
    ui.mainPanel = document.querySelector(".main-panel");
    ui.sourceGroups = document.getElementById("source-groups");
    ui.sourceSearchInput = document.getElementById("source-search-input");
    ui.sidebarPanel = document.getElementById("sidebar-panel");
    ui.sidebarBackdrop = document.getElementById("sidebar-backdrop");
    ui.navGroupRow = document.getElementById("nav-group-row");
    ui.navSectionRow = document.getElementById("nav-section-row");
    ui.breadcrumbLine = document.getElementById("breadcrumb-line");
    ui.summaryStrip = document.getElementById("summary-strip");
    ui.screenPanel = document.querySelector(".screen-panel");
    ui.screenRoot = document.getElementById("screen-root");
    ui.screenEyebrow = document.getElementById("screen-eyebrow");
    ui.screenTitle = document.getElementById("screen-title");
    ui.screenCaption = document.getElementById("screen-caption");
    ui.tasksGroups = document.getElementById("tasks-groups");
    ui.jobDetailCard = document.getElementById("job-detail-card");
    ui.toastStack = document.getElementById("toast-stack");
    ui.toggleSourcesBtn = document.getElementById("toggle-sources-btn");
    ui.toggleTasksBtn = document.getElementById("toggle-tasks-btn");
    ui.schedulerToggleBtn = document.getElementById("scheduler-toggle-btn");
    ui.tasksPanel = document.getElementById("tasks-panel");
    ui.tasksBackdrop = document.getElementById("tasks-backdrop");
    ui.relationMapOverlayHost = document.getElementById("relation-map-overlay-host");
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

  function screenUsesDetailDrawer(section) {
    return ["content", "search", "claims", "cases", "events", "review_ops", "entities", "relations", "officials"].includes(section);
  }

  function relationMapOverlayOpen() {
    return state.section === "relations" && (state.filters.relations.view || "cards") === "map";
  }

  function interactiveRoots() {
    const roots = [ui.screenRoot];
    if (relationMapOverlayOpen() && ui.relationMapOverlayHost && !ui.relationMapOverlayHost.hidden) {
      roots.unshift(ui.relationMapOverlayHost);
    }
    return roots.filter(Boolean);
  }

  function queryInteractiveAll(selector) {
    return interactiveRoots().flatMap((root) => [...root.querySelectorAll(selector)]);
  }

  function queryInteractive(selector) {
    for (const root of interactiveRoots()) {
      const found = root.querySelector(selector);
      if (found) {
        return found;
      }
    }
    return null;
  }

  function isDetailDrawerOpen(section, detail) {
    return !!detail && screenUsesDetailDrawer(section) && !!state.detailDrawerOpen[section];
  }

  function openDetailDrawer(section) {
    if (screenUsesDetailDrawer(section)) {
      state.detailDrawerOpen[section] = true;
    }
  }

  function closeDetailDrawer(section) {
    state.detailDrawerOpen[section] = false;
  }

  function bindShellEvents() {
    document.getElementById("manual-refresh-btn").addEventListener("click", manualRefresh);
    document.getElementById("refresh-sources-btn").addEventListener("click", requestSources);
    document.getElementById("export-obsidian-btn").addEventListener("click", () => {
      bridgeVoid("exportObsidian");
    });

    ui.toggleSourcesBtn.addEventListener("click", () => {
      state.sidebarCollapsed = !state.sidebarCollapsed;
      renderShell();
      scheduleRelationMapStageSync();
    });

    if (ui.sidebarBackdrop) {
      ui.sidebarBackdrop.addEventListener("click", () => {
        state.sidebarCollapsed = true;
        renderShell();
        scheduleRelationMapStageSync();
      });
    }

    ui.toggleTasksBtn.addEventListener("click", () => {
      state.tasksCollapsed = !state.tasksCollapsed;
      renderShell();
      scheduleRelationMapStageSync();
    });

    if (ui.tasksBackdrop) {
      ui.tasksBackdrop.addEventListener("click", () => {
        state.tasksCollapsed = true;
        renderShell();
        scheduleRelationMapStageSync();
      });
    }

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

    document.addEventListener("keydown", async (event) => {
      if (event.key !== "Escape") {
        return;
      }
      if (closeVisibleGraphPopover()) {
        return;
      }
      if (relationMapOverlayOpen() && state.detailDrawerOpen.relations) {
        closeDetailDrawer("relations");
        delete state.selectedRows.relations;
        renderScreen();
        return;
      }
      if (!state.sidebarCollapsed) {
        state.sidebarCollapsed = true;
        renderShell();
        scheduleRelationMapStageSync();
        return;
      }
      if (!state.tasksCollapsed) {
        state.tasksCollapsed = true;
        renderShell();
        scheduleRelationMapStageSync();
      }
    });

    window.addEventListener("resize", () => {
      scheduleRelationMapStageSync();
    });
  }

  function scheduleRelationMapStageSync() {
    requestAnimationFrame(() => {
      syncRelationMapStage();
      refitVisibleGraphs();
    });
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

    ui.appShell.classList.toggle("sources-open", !state.sidebarCollapsed);
    ui.appShell.classList.toggle("sources-collapsed", state.sidebarCollapsed);
    ui.appShell.classList.toggle("tasks-open", !state.tasksCollapsed);
    ui.appShell.classList.toggle("tasks-collapsed", state.tasksCollapsed);
    ui.appShell.dataset.group = state.group;
    ui.appShell.dataset.section = state.section;
    ui.toggleSourcesBtn.textContent = "Источники";
    ui.toggleTasksBtn.textContent = "Панель";
    ui.toggleSourcesBtn.classList.toggle("active", !state.sidebarCollapsed);
    ui.toggleTasksBtn.classList.toggle("active", !state.tasksCollapsed);
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

  function groupKeyForSection(section) {
    for (const group of state.bootstrap?.navigation || []) {
      if ((group.sections || []).some((item) => item.key === section)) {
        return group.key;
      }
    }
    return state.group;
  }

  function shouldShowSummaryStrip() {
    return state.group === "monitoring" && state.section === "overview";
  }

  function renderSummary() {
    const visible = shouldShowSummaryStrip();
    ui.mainPanel?.classList.toggle("summary-hidden", !visible);
    ui.summaryStrip.hidden = !visible;
    ui.summaryStrip.classList.toggle("is-hidden", !visible);
    if (!visible) {
      ui.summaryStrip.innerHTML = "";
      return;
    }
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
          <div class="eyebrow">Рабочая задача</div>
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
        ${compact ? "" : `<span class="job-state-label">${title}</span>`}
      </span>
    `;
  }

  function renderScreen() {
    if (!state.screenData) {
      return;
    }
    ui.appShell.dataset.group = state.group;
    ui.appShell.dataset.section = state.section;
    ui.screenPanel?.classList.remove("relation-map-host");
    ui.screenRoot.classList.remove("relation-map-host");
    clearRelationMapOverlay();
    renderSummary();
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
      case "events":
        renderEventsScreen(state.screenData);
        break;
      case "review_ops":
        renderReviewOpsScreen(state.screenData);
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
    bindDetailDrawer();
    bindJumpLinks();
    bindEvidenceGraphs();
  }

  function clearRelationMapOverlay() {
    if (!ui.relationMapOverlayHost) {
      return;
    }
    ui.relationMapOverlayHost.innerHTML = "";
    ui.relationMapOverlayHost.hidden = true;
    ui.relationMapOverlayHost.setAttribute("aria-hidden", "true");
    ui.relationMapOverlayHost.classList.remove("open");
  }

  function renderOverviewScreen(payload) {
    const recentContent = payload.recent_content || [];
    const recentCases = payload.recent_cases || [];
    const counts = payload.secondary_counts || {};
    const graphHealth = payload.graph_health || {};
    const runtimeHealth = payload.runtime_health || {};
    const lowAccountability = payload.low_accountability || [];
    const countOrder = [
      "events",
      "facts",
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
      events: "События",
      facts: "Факты",
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
      events: "канонические события",
      facts: "содержательные факты",
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
    const drawerOpen = isDetailDrawerOpen(section, payload.detail);
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
            ${renderLinkSection(
              "Сущности",
              payload.detail.entities,
              (entity) => `${escapeHtml(entity.canonical_name)} · ${escapeHtml(entity.mention_type)}`,
              { resolveJump: (entity) => ({ screen: "entities", id: entity.id }) }
            )}
            ${renderLinkSection(
              "Claims",
              payload.detail.claims,
              (claim) => escapeHtml(claim.claim_text),
              { resolveJump: (claim) => ({ screen: "claims", id: claim.id }) }
            )}
          `
        : emptyState("Нет выбранного объекта", "Выберите запись слева."),
      selectionBanner: drawerOpen && payload.detail
        ? renderSelectionBanner(
            payload.detail.title || "Контент",
            [payload.detail.source_name || "", formatDate(payload.detail.published_at)].filter(Boolean).join(" · ")
          )
        : "",
      detailOpen: drawerOpen,
    });
    bindTextFilter(section);
    bindRowSelection(section);
  }

  function renderClaimsScreen(payload) {
    const query = state.filters.claims.query || "";
    const status = state.filters.claims.status || "";
    const drawerOpen = isDetailDrawerOpen("claims", payload.detail);
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
            ${renderEvidenceGraphSection(payload.detail.evidence_graph, "Evidence graph")}
            ${
              !payload.detail.evidence_graph
                ? renderLinkSection(
                    "Evidence",
                    payload.detail.evidence,
                    (item) => `${escapeHtml(item.evidence_type || "evidence")} · ${escapeHtml(item.evidence_title || "—")}`,
                    {
                      resolveJump: (item) =>
                        item.evidence_item_id ? { screen: "content", id: item.evidence_item_id } : null,
                    }
                  )
                : ""
            }
          `
        : emptyState("Нет выбранного claim", "Выберите запись слева."),
      selectionBanner: drawerOpen && payload.detail
        ? renderSelectionBanner(
            truncate(payload.detail.claim_text || "Claim", 84),
            payload.detail.status || "claim"
          )
        : "",
      detailOpen: drawerOpen,
    });
    bindTextFilter("claims");
    queryInteractiveAll("[data-claim-status]").forEach((button) => {
      button.addEventListener("click", async () => {
        state.filters.claims.status = button.dataset.claimStatus;
        await loadCurrentScreen();
      });
    });
    bindRowSelection("claims");
  }

  function renderCasesScreen(payload) {
    const query = state.filters.cases.query || "";
    const drawerOpen = isDetailDrawerOpen("cases", payload.detail);
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
            ${renderLinkSection(
              "Claims",
              payload.detail.claims,
              (item) => `${escapeHtml(item.claim_text)}${Number(item.support_count || 0) > 1 ? ` <span class="support-pill">×${escapeHtml(String(item.support_count))}</span>` : ""}`,
              {
                resolveJump: (item) => ({ screen: "claims", id: item.id }),
                secondary: (item) =>
                  escapeHtml(
                    [
                      item.content_title || "",
                      item.status || "",
                      Number(item.evidence_count || 0) ? `evidence ${item.evidence_count}` : "",
                    ]
                      .filter(Boolean)
                      .join(" · ")
                  ),
                footer:
                  Number(payload.detail.claims_hidden_count || 0) > 0
                    ? `Скрыто повторов и низкосигнальных claims: ${escapeHtml(String(payload.detail.claims_hidden_count))}`
                    : "",
              }
            )}
            ${renderLinkSection(
              "Events",
              payload.detail.events,
              (item) => `${escapeHtml(formatDate(item.event_date))} · ${escapeHtml(item.event_title || "—")}`,
              {
                resolveJump: (item) =>
                  item.content_item_id ? { screen: "content", id: item.content_item_id } : null,
              }
            )}
          `
        : emptyState("Нет выбранного дела", "Выберите case слева."),
      selectionBanner: drawerOpen && payload.detail
        ? renderSelectionBanner(
            payload.detail.title || "Дело",
            payload.detail.case_type || payload.detail.status || "case"
          )
        : "",
      detailOpen: drawerOpen,
    });
    bindTextFilter("cases");
    bindRowSelection("cases");
  }

  function renderEventsScreen(payload) {
    const query = state.filters.events.query || "";
    const drawerOpen = isDetailDrawerOpen("events", payload.detail);
    ui.screenRoot.innerHTML = renderMasterDetailLayout({
      filters: `
        <div class="screen-filters">
          <input id="screen-query-input" class="glass-input" type="search" placeholder="Поиск по событиям" value="${escapeHtml(query)}">
        </div>
      `,
      list: renderTableList(
        payload.items,
        (item) => `
          <div class="table-row-head">
            <div class="table-primary">${escapeHtml(item.canonical_title || "Событие")}</div>
            <span class="badge ${badgeClass(item.status || "active")}">${escapeHtml(item.event_type || item.status || "—")}</span>
          </div>
          <div class="table-secondary">${escapeHtml(formatDate(item.event_date_start))}${item.event_date_end ? ` → ${escapeHtml(
            formatDate(item.event_date_end)
          )}` : ""} · importance ${escapeHtml(Number(item.importance_score || 0).toFixed(2))}</div>
          ${
            item.summary_short
              ? `<div class="table-secondary">${escapeHtml(truncate(item.summary_short, 160))}</div>`
              : ""
          }
        `
      ),
      detail: payload.detail
        ? `
            <h3 class="detail-title">${escapeHtml(payload.detail.canonical_title || "Событие")}</h3>
            <div class="detail-grid">
              <div class="detail-kv"><div class="k">Тип</div><div class="v">${escapeHtml(payload.detail.event_type || "—")}</div></div>
              <div class="detail-kv"><div class="k">Статус</div><div class="v">${escapeHtml(payload.detail.status || "—")}</div></div>
              <div class="detail-kv"><div class="k">Интервал</div><div class="v">${escapeHtml(formatDate(payload.detail.event_date_start))}${
                payload.detail.event_date_end ? ` → ${escapeHtml(formatDate(payload.detail.event_date_end))}` : ""
              }</div></div>
              <div class="detail-kv"><div class="k">Важность</div><div class="v">${escapeHtml(
                Number(payload.detail.importance_score || 0).toFixed(2)
              )}</div></div>
            </div>
            ${
              payload.detail.summary_short
                ? `<div class="detail-section"><h3>Кратко</h3><div class="muted">${escapeHtml(payload.detail.summary_short)}</div></div>`
                : ""
            }
            ${
              payload.detail.summary_long
                ? `<div class="detail-section"><h3>Нарратив</h3><div class="muted">${escapeHtml(payload.detail.summary_long)}</div></div>`
                : ""
            }
            ${renderLinkSection(
              "Таймлайн",
              payload.detail.timeline,
              (item) => `${escapeHtml(formatDate(item.timeline_date))} · ${escapeHtml(item.title || "—")}`,
              {
                secondary: (item) => escapeHtml(item.description || "—"),
                resolveJump: (item) =>
                  item.content_item_id ? { screen: "content", id: item.content_item_id } : null,
              }
            )}
            ${renderLinkSection(
              "Участники",
              payload.detail.entities,
              (item) => `${escapeHtml(item.canonical_name || "—")} · ${escapeHtml(item.role || "role")}`,
              {
                secondary: (item) =>
                  escapeHtml(
                    [
                      item.entity_type || "",
                      item.valid_from ? `c ${formatDate(item.valid_from)}` : "",
                      item.valid_to ? `до ${formatDate(item.valid_to)}` : "",
                    ]
                      .filter(Boolean)
                      .join(" · ")
                  ),
                resolveJump: (item) => ({ screen: "entities", id: item.entity_id || item.id }),
              }
            )}
            ${renderLinkSection(
              "Факты",
              payload.detail.facts,
              (item) => `${escapeHtml(item.fact_type || "fact")} · ${escapeHtml(item.canonical_text || "—")}`,
              {
                secondary: (item) =>
                  escapeHtml(
                    [
                      item.polarity || "",
                      item.valid_from ? `c ${formatDate(item.valid_from)}` : "",
                      item.valid_to ? `до ${formatDate(item.valid_to)}` : "",
                    ]
                      .filter(Boolean)
                      .join(" · ")
                  ),
              }
            )}
            ${renderLinkSection(
              "Материалы",
              payload.detail.items,
              (item) => `${escapeHtml(item.title || "—")} · ${escapeHtml(item.item_role || "item")}`,
              {
                secondary: (item) =>
                  escapeHtml(
                    [item.source_name || "", formatDate(item.published_at), item.source_strength || ""]
                      .filter(Boolean)
                      .join(" · ")
                  ),
                resolveJump: (item) => ({ screen: "content", id: item.content_item_id || item.id }),
              }
            )}
          `
        : emptyState("Нет выбранного события", "Выберите событие слева."),
      selectionBanner: drawerOpen && payload.detail
        ? renderSelectionBanner(
            payload.detail.canonical_title || "Событие",
            [payload.detail.event_type || "", formatDate(payload.detail.event_date_start)].filter(Boolean).join(" · ")
          )
        : "",
      detailOpen: drawerOpen,
    });
    bindTextFilter("events");
    bindRowSelection("events");
  }

  function renderReviewOpsScreen(payload) {
    const query = state.filters.review_ops.query || "";
    const queue = state.filters.review_ops.queue || "";
    const status = state.filters.review_ops.status || "open";
    const drawerOpen = isDetailDrawerOpen("review_ops", payload.detail);
    const queueButtons = [
      ["", "Все"],
      ...((payload.queues || []).map((item) => [item.queue_key, `${item.queue_key} (${item.open_total || item.total || 0})`])),
    ];
    ui.screenRoot.innerHTML = renderMasterDetailLayout({
      filters: `
        <div class="screen-filters">
          <input id="screen-query-input" class="glass-input" type="search" placeholder="Поиск по review tasks" value="${escapeHtml(query)}">
          <div class="status-filter-row">
            ${queueButtons
              .map(
                ([value, label]) => `
                  <button class="filter-chip ${queue === value ? "active" : ""}" data-review-queue="${escapeHtml(value)}">
                    ${escapeHtml(label)}
                  </button>
                `
              )
              .join("")}
          </div>
          <div class="status-filter-row">
            ${["open", "needs_review", "resolved", ""]
              .map(
                (value) => `
                  <button class="filter-chip ${status === value || (!value && !status) ? "active" : ""}" data-review-status="${escapeHtml(value)}">
                    ${escapeHtml(value || "Все статусы")}
                  </button>
                `
              )
              .join("")}
          </div>
        </div>
      `,
      list: renderTableList(
        payload.items,
        (item) => `
          <div class="table-row-head">
            <div class="table-primary">${escapeHtml(item.queue_key || "queue")} · ${escapeHtml(item.subject_type || "item")}</div>
            <span class="badge ${badgeClass(item.status)}">${escapeHtml(item.status || "—")}</span>
          </div>
          <div class="table-secondary">${escapeHtml(item.suggested_action || "—")} · confidence ${escapeHtml(
            Number(item.confidence || 0).toFixed(2)
          )}</div>
          <div class="table-secondary">${escapeHtml(item.machine_reason || item.task_key || "—")}</div>
        `
      ),
      detail: payload.detail
        ? `
            <h3 class="detail-title">${escapeHtml(payload.detail.queue_key || "Review task")}</h3>
            <div class="detail-grid">
              <div class="detail-kv"><div class="k">Статус</div><div class="v">${escapeHtml(payload.detail.status || "—")}</div></div>
              <div class="detail-kv"><div class="k">Действие</div><div class="v">${escapeHtml(payload.detail.suggested_action || "—")}</div></div>
              <div class="detail-kv"><div class="k">Confidence</div><div class="v">${escapeHtml(
                Number(payload.detail.confidence || 0).toFixed(2)
              )}</div></div>
              <div class="detail-kv"><div class="k">Пакет</div><div class="v">${escapeHtml(payload.detail.review_pack_id || "—")}</div></div>
            </div>
            ${
              payload.detail.subject_summary
                ? `<div class="detail-section"><h3>Объект</h3><div class="muted">${escapeHtml(payload.detail.subject_summary)}</div></div>`
                : ""
            }
            ${
              payload.detail.machine_reason
                ? `<div class="detail-section"><h3>Machine reason</h3><div class="muted">${escapeHtml(payload.detail.machine_reason)}</div></div>`
                : ""
            }
            ${
              payload.detail.source_links?.length
                ? `<div class="detail-section"><h3>Source links</h3><ul class="detail-link-list">${payload.detail.source_links
                    .map(
                      (link) => `
                        <li><a class="inline-link static" href="${escapeHtml(link)}" target="_blank" rel="noreferrer">${escapeHtml(link)}</a></li>
                      `
                    )
                    .join("")}</ul></div>`
                : ""
            }
            <div class="detail-section"><h3>Payload</h3><pre class="json-block">${escapeHtml(
              payload.detail.candidate_payload_pretty || "{}"
            )}</pre></div>
          `
        : emptyState("Нет review task", "Выберите запись слева."),
      selectionBanner: drawerOpen && payload.detail
        ? renderSelectionBanner(
            payload.detail.queue_key || "Review task",
            payload.detail.subject_type || payload.detail.suggested_action || "review"
          )
        : "",
      detailOpen: drawerOpen,
    });
    bindTextFilter("review_ops");
    queryInteractiveAll("[data-review-queue]").forEach((button) => {
      button.addEventListener("click", async () => {
        state.filters.review_ops.queue = button.dataset.reviewQueue;
        await loadCurrentScreen();
      });
    });
    queryInteractiveAll("[data-review-status]").forEach((button) => {
      button.addEventListener("click", async () => {
        state.filters.review_ops.status = button.dataset.reviewStatus;
        await loadCurrentScreen();
      });
    });
    bindRowSelection("review_ops");
  }

  function renderEntitiesScreen(payload) {
    const query = state.filters.entities.query || "";
    const drawerOpen = isDetailDrawerOpen("entities", payload.detail);
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
      selectionBanner: drawerOpen && payload.detail
        ? renderSelectionBanner(
            payload.detail.canonical_name || payload.detail.full_name || "Entity",
            payload.detail.description || payload.detail.entity_type || "entity"
          )
        : "",
      detailOpen: drawerOpen,
    });
    bindTextFilter("entities");
    bindRowSelection("entities");
  }

  function renderRelationsScreen(payload) {
    const query = state.filters.relations.query || "";
    const layer = state.filters.relations.layer || "";
    const view = state.filters.relations.view || "cards";
    const mapGroup = state.filters.relations.map_group || "";
    const drawerOpen = isDetailDrawerOpen("relations", payload.detail);
    const filtersMarkup = `
      <div class="screen-filters relation-filters">
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
    `;
    const detailMarkup = payload.detail
        ? `
            <h3 class="detail-title">${escapeHtml(payload.detail.from_name)} → ${escapeHtml(payload.detail.to_name)}</h3>
            <div class="detail-grid">
              <div class="detail-kv"><div class="k">Тип связи</div><div class="v">${escapeHtml(payload.detail.relation_label || payload.detail.relation_type)}</div></div>
              <div class="detail-kv"><div class="k">Слой</div><div class="v">${escapeHtml(payload.detail.layer_label || payload.detail.layer)}</div></div>
              <div class="detail-kv"><div class="k">Сила</div><div class="v">${escapeHtml(payload.detail.strength || "—")}</div></div>
              <div class="detail-kv"><div class="k">Основание</div><div class="v">${escapeHtml(payload.detail.detected_label || payload.detail.detected_by || "—")}</div></div>
            </div>
            <div class="detail-section">
              <h3>Почему связаны</h3>
              <div class="muted">${escapeHtml(payload.detail.summary || "Причина связи не указана.")}</div>
            </div>
            <div class="detail-grid">
              <div class="detail-kv">
                <div class="k">Откуда</div>
                <div class="v">${renderActionLink(payload.detail.from_name || "—", "entities", payload.detail.from_entity_id)}</div>
              </div>
              <div class="detail-kv">
                <div class="k">Куда</div>
                <div class="v">${renderActionLink(payload.detail.to_name || "—", "entities", payload.detail.to_entity_id)}</div>
              </div>
            </div>
            ${
              payload.detail.context_title
                ? `<div class="detail-section"><h3>Контекст</h3><div class="muted">${escapeHtml(payload.detail.context_title)}</div></div>`
                : ""
            }
            ${
              payload.detail.temporal_window &&
              Object.values(payload.detail.temporal_window || {}).some((value) => value)
                ? `<div class="detail-section"><h3>Время</h3><div class="muted">${escapeHtml(
                    [
                      payload.detail.temporal_window.valid_from ? `valid_from: ${formatDate(payload.detail.temporal_window.valid_from)}` : "",
                      payload.detail.temporal_window.valid_to ? `valid_to: ${formatDate(payload.detail.temporal_window.valid_to)}` : "",
                      payload.detail.temporal_window.observed_at ? `observed: ${formatDate(payload.detail.temporal_window.observed_at)}` : "",
                      payload.detail.temporal_window.recorded_at ? `recorded: ${formatDate(payload.detail.temporal_window.recorded_at)}` : "",
                    ]
                      .filter(Boolean)
                      .join(" · ")
                  )}</div></div>`
                : ""
            }
            ${
              payload.detail.evidence_mix && Object.keys(payload.detail.evidence_mix).length
                ? `<div class="detail-section"><h3>Evidence mix</h3><pre class="json-block">${escapeHtml(
                    JSON.stringify(payload.detail.evidence_mix, null, 2)
                  )}</pre></div>`
                : ""
            }
            ${renderBridgePathSection(payload.detail.bridge_paths)}
            ${renderEvidenceGraphSection(payload.detail.evidence_graph, "Evidence graph")}
          `
        : emptyState("Нет выбранной связи", "Выберите relation слева.");
    if (view === "map") {
      ui.screenPanel?.classList.add("relation-map-host");
      ui.screenRoot.classList.add("relation-map-host");
      ui.screenRoot.innerHTML = "";
      if (ui.relationMapOverlayHost) {
        ui.relationMapOverlayHost.hidden = false;
        ui.relationMapOverlayHost.setAttribute("aria-hidden", "false");
        ui.relationMapOverlayHost.classList.add("open");
        ui.relationMapOverlayHost.innerHTML = renderRelationMapScreen({
          filters: filtersMarkup,
          graph: renderRelationMapSection(payload.map_graph, mapGroup),
          detail: detailMarkup,
          detailOpen: drawerOpen,
        });
      }
      scheduleRelationMapStageSync();
    } else {
      ui.screenRoot.innerHTML = renderMasterDetailLayout({
        filters: filtersMarkup,
        list:
          view === "table"
            ? renderDataTable({
                items: payload.items,
                columns: ["Откуда", "Связь", "Куда", "Слой", "Основание"],
                rowId: (item) => item.id,
                rowCells: (item) => [
                  escapeHtml(item.from_name || "—"),
                  escapeHtml(item.relation_label || item.relation_type || "—"),
                  escapeHtml(item.to_name || "—"),
                  `<span class="badge ${relationLayerBadgeClass(item.layer)}">${escapeHtml(item.layer_label || item.layer || "—")}</span>`,
                  escapeHtml(item.detected_label || item.detected_by || "—"),
                ],
              })
            : renderTableList(
                payload.items,
                (item) => `
                  <div class="table-row-head">
                    <div class="table-primary">${escapeHtml(item.from_name)} → ${escapeHtml(item.to_name)}</div>
                    <span class="badge ${relationLayerBadgeClass(item.layer)}">
                      ${escapeHtml(item.layer_label || item.layer)}
                    </span>
                  </div>
                  <div class="table-secondary">${escapeHtml(item.relation_label || item.relation_type)} · ${escapeHtml(item.detected_label || item.detected_by || "—")}</div>
                `
              ),
        detail: detailMarkup,
        selectionBanner: drawerOpen && payload.detail
          ? renderSelectionBanner(
              `${payload.detail.from_name || "—"} → ${payload.detail.to_name || "—"}`,
              payload.detail.relation_label || payload.detail.relation_type || "Связь"
            )
          : "",
        detailOpen: drawerOpen,
      });
    }
    bindTextFilter("relations");
    queryInteractiveAll("[data-relation-layer]").forEach((button) => {
      button.addEventListener("click", async () => {
        state.filters.relations.layer = button.dataset.relationLayer;
        await loadCurrentScreen();
      });
    });
    bindViewSwitch("relations");
    bindRowSelection("relations");
    queryInteractiveAll("[data-map-group]").forEach((button) => {
      button.addEventListener("click", () => {
        state.filters.relations.map_group = button.dataset.mapGroup || "";
        renderScreen();
      });
    });
  }

  function renderOfficialsScreen(payload) {
    const query = state.filters.officials.query || "";
    const view = state.filters.officials.view || "cards";
    const drawerOpen = isDetailDrawerOpen("officials", payload.detail);
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
      selectionBanner: drawerOpen && payload.detail
        ? renderSelectionBanner(
            payload.detail.canonical_name || payload.detail.full_name || "Профиль",
            payload.detail.positions?.[0]?.organization || payload.detail.description || "Руководство"
          )
        : "",
      detailOpen: drawerOpen,
    });
    bindTextFilter("officials");
    bindViewSwitch("officials");
    bindRowSelection("officials", "entity_id");
  }

  function renderSettingsScreen(payload) {
    const items = payload.items || [];
    const detail = payload.detail || {};
    ui.screenRoot.innerHTML = `
      <div class="settings-screen">
        <section class="overview-panel settings-hero-panel">
          <div class="eyebrow">Workspace</div>
          <h3 class="hero-title">Публичные настройки и базовые пути текущего окружения</h3>
          <div class="detail-grid settings-summary-grid">
            <div class="detail-kv">
              <div class="k">PROJECT_ROOT</div>
              <div class="v">${escapeHtml(detail.project_root || "—")}</div>
            </div>
            <div class="detail-kv">
              <div class="k">DB_PATH</div>
              <div class="v">${escapeHtml(
                String(items.find((item) => item.key === "db_path")?.value ?? "db/news_unified.db")
              )}</div>
            </div>
            <div class="detail-kv">
              <div class="k">OBSIDIAN_EXPORT_DIR</div>
              <div class="v">${escapeHtml(
                String(items.find((item) => item.key === "obsidian_export_dir")?.value ?? "—")
              )}</div>
            </div>
          </div>
        </section>

        <section class="overview-panel settings-list-panel">
          <h3>Runtime settings</h3>
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
        </section>
      </div>
    `;
  }

  function renderEntityDetail(detail) {
    const entityId = detail.entity_id || detail.id;
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
      ${renderLinkSection(
        "Фото",
        detail.media,
        (item) => escapeHtml(item.media_kind || "media"),
        {
          secondary: (item) => escapeHtml(item.file_path || item.source_url || "—"),
        }
      )}
      ${renderLinkSection(
        "Связанный контент",
        detail.content,
        (item) => `${escapeHtml(formatDate(item.published_at))} · ${escapeHtml(item.title || "—")}`,
        { resolveJump: (item) => ({ screen: "content", id: item.id }) }
      )}
      ${renderLinkSection(
        "Claims",
        detail.claims,
        (item) => `${escapeHtml(item.claim_text || "—")}${Number(item.support_count || 0) > 1 ? ` <span class="support-pill">×${escapeHtml(String(item.support_count))}</span>` : ""}`,
        {
          resolveJump: (item) => ({ screen: "claims", id: item.id }),
          secondary: (item) =>
            escapeHtml(
              [
                item.content_title || "",
                item.status || "",
                Number(item.evidence_count || 0) ? `evidence ${item.evidence_count}` : "",
              ]
                .filter(Boolean)
                .join(" · ")
            ),
        }
      )}
      ${renderLinkSection(
        "Cases",
        detail.cases,
        (item) => escapeHtml(item.title || "—"),
        { resolveJump: (item) => ({ screen: "cases", id: item.id }) }
      )}
      ${renderLinkSection(
        "Disclosures",
        detail.disclosures,
        (item) => `${escapeHtml(String(item.disclosure_year || "—"))} · доход ${escapeHtml(String(item.income_amount ?? item.raw_income_text ?? "—"))}`,
        {
          secondary: (item) => escapeHtml(item.source_url || "—"),
        }
      )}
      ${renderLinkSection(
        "Affiliations",
        detail.affiliations,
        (item) => `${escapeHtml(item.role_type || "—")} · ${escapeHtml(item.company_name || "—")}`,
        {
          secondary: (item) => escapeHtml(item.source_url || item.evidence_class || "—"),
        }
      )}
      ${renderLinkSection(
        "Restrictions",
        detail.restrictions,
        (item) => `${escapeHtml(item.restriction_type || "—")} · ${escapeHtml(item.target_name || "—")}`,
        {
          secondary: (item) => escapeHtml(item.right_category || item.stated_justification || "—"),
        }
      )}
      ${renderLinkSection(
        "Связи",
        detail.relations,
        (item) => `${escapeHtml(item.relation_label || item.relation_type || "—")} · ${escapeHtml(relationPeerName(item, entityId))}`,
        {
          resolveJump: (item) => ({ screen: "relations", id: item.id }),
          secondary: (item) =>
            escapeHtml(
              [
                item.layer_label || item.layer || "",
                item.detected_label || item.detected_by || "",
                item.context_title || "",
              ]
                .filter(Boolean)
                .join(" · ")
            ),
        }
      )}
    `;
  }

  function renderDetailDrawer(detail, detailOpen, options = {}) {
    const variant = options.variant ? ` ${options.variant}` : "";
    return `
      <div class="detail-drawer-scrim${variant} ${detailOpen ? "open" : ""}" data-detail-close></div>
      <aside class="detail-drawer${variant} ${detailOpen ? "open" : ""}" data-detail-drawer aria-hidden="${detailOpen ? "false" : "true"}">
        <div class="detail-drawer-head">
          <div>
            <div class="eyebrow">Детали</div>
            <div class="detail-drawer-title">Выбранный объект</div>
          </div>
          <button class="ghost-chip detail-drawer-close" data-detail-close type="button">Закрыть</button>
        </div>
        <section class="detail-pane detail-drawer-body">
          ${detail || emptyState("Нет выбранного объекта", "Выберите запись слева.")}
        </section>
      </aside>
    `;
  }

  function renderMasterDetailLayout({ filters, list, detail, selectionBanner = "", detailOpen = false }) {
    const drawerEnabled = screenUsesDetailDrawer(state.section);
    return `
      <div class="screen-stack ${drawerEnabled ? "drawer-enabled" : ""} ${detailOpen ? "drawer-open" : ""}">
        <section class="master-pane master-pane-overlay">
          ${filters || ""}
          <div class="master-list-wrap ${selectionBanner ? "has-banner" : ""}">
            ${selectionBanner || ""}
            <div class="table-list">${list}</div>
          </div>
        </section>
        ${drawerEnabled ? renderDetailDrawer(detail, detailOpen) : ""}
      </div>
    `;
  }

  function renderRelationMapScreen({ filters, graph, detail, detailOpen }) {
    return `
      <div class="relation-map-screen ${detailOpen ? "drawer-open" : ""}">
        ${filters || ""}
        <div class="relation-map-stage">
          <div class="relation-map-surface glass-panel">
            ${graph}
          </div>
          ${renderDetailDrawer(detail, detailOpen, { variant: "map-variant" })}
        </div>
      </div>
    `;
  }

  function renderViewSwitch(section, currentView) {
    const options =
      section === "relations"
        ? [
            ["cards", "Карточки"],
            ["table", "Таблица"],
            ["map", "Карта"],
          ]
        : [
            ["cards", "Карточки"],
            ["table", "Таблица"],
          ];
    return `
      <div class="view-switch" data-view-switch="${escapeHtml(section)}">
        ${options
          .map(
            ([value, label]) => `
              <button class="view-switch-btn ${currentView === value ? "active" : ""}" data-screen-view="${escapeHtml(
                section
              )}:${escapeHtml(value)}" type="button">${escapeHtml(label)}</button>
            `
          )
          .join("")}
      </div>
    `;
  }

  function renderSelectionBanner(title, subtitle) {
    return `
      <div class="selection-banner">
        <div class="selection-banner-title">${escapeHtml(title || "Выбран объект")}</div>
        <div class="selection-banner-subtitle">${escapeHtml(subtitle || "")}</div>
      </div>
    `;
  }

  function renderActionLink(label, screen, id) {
    if (!id) {
      return escapeHtml(label || "—");
    }
    return `<button class="inline-link compact" data-jump-screen="${escapeHtml(screen)}" data-jump-id="${escapeHtml(
      String(id)
    )}" type="button">${escapeHtml(label || "—")}</button>`;
  }

  function relationLayerBadgeClass(layer) {
    if (layer === "structural") {
      return "emerald";
    }
    if (layer === "weak_similarity") {
      return "amber";
    }
    return "cyan";
  }

  function relationMapNodeGroup(node) {
    if (!node) {
      return { key: "other", label: "Прочее", tone: "other" };
    }
    if (node.group_key) {
      return {
        key: String(node.group_key),
        label: String(node.group_label || node.group_key),
        tone: String(node.group_tone || node.group_key || "other"),
      };
    }
    const role = String(node.role || "");
    const label = String(node.label || "").toLowerCase();
    if (role === "map_entity" || role === "entity" || role === "entity_from" || role === "entity_to") {
      if (label === "person") {
        return { key: "people", label: "Персоны", tone: "people" };
      }
      if (label === "organization") {
        return { key: "organizations", label: "Организации", tone: "organizations" };
      }
      return { key: "entities", label: "Сущности", tone: "entities" };
    }
    if (role === "claim" || role === "bridge_claim" || role === "relation") {
      return { key: "claims", label: "Заявления", tone: "claims" };
    }
    if (role === "case" || role === "bridge_case") {
      return { key: "cases", label: "Дела", tone: "cases" };
    }
    if (role === "bridge_bill") {
      return { key: "bills", label: "Законопроекты", tone: "bills" };
    }
    if (role === "bridge_contract") {
      return { key: "contracts", label: "Контракты", tone: "contracts" };
    }
    if (role === "bridge_affiliation") {
      return { key: "affiliations", label: "Аффилиации", tone: "affiliations" };
    }
    if (role === "bridge_restriction") {
      return { key: "restrictions", label: "Ограничения", tone: "restrictions" };
    }
    if (role === "content_origin" || role === "bridge_content" || role === "bridge_evidence" || role === "evidence" || role === "context") {
      return { key: "documents", label: "Документы", tone: "documents" };
    }
    return { key: "other", label: "Прочее", tone: "other" };
  }

  function relationMapGroups(graph) {
    const priority = [
      "people",
      "organizations",
      "entities",
      "claims",
      "cases",
      "bills",
      "contracts",
      "affiliations",
      "restrictions",
      "documents",
      "other",
    ];
    const buckets = new Map();
    (graph?.nodes || []).forEach((node) => {
      const group = relationMapNodeGroup(node);
      const bucket = buckets.get(group.key) || { ...group, count: 0 };
      bucket.count += 1;
      buckets.set(group.key, bucket);
    });
    return [...buckets.values()].sort((a, b) => {
      const aIndex = priority.indexOf(a.key);
      const bIndex = priority.indexOf(b.key);
      return (aIndex === -1 ? 999 : aIndex) - (bIndex === -1 ? 999 : bIndex);
    });
  }

  function filterRelationMapGraph(graph, activeGroup) {
    if (!graph || !activeGroup) {
      return graph;
    }
    const focusIds = new Set();
    (graph.nodes || []).forEach((node) => {
      if (relationMapNodeGroup(node).key === activeGroup) {
        focusIds.add(node.id);
      }
    });
    if (!focusIds.size) {
      return graph;
    }
    const visibleIds = new Set(focusIds);
    (graph.edges || []).forEach((edge) => {
      if (focusIds.has(edge.from) || focusIds.has(edge.to)) {
        visibleIds.add(edge.from);
        visibleIds.add(edge.to);
      }
    });
    const nodes = (graph.nodes || []).filter((node) => visibleIds.has(node.id));
    const nodeIds = new Set(nodes.map((node) => node.id));
    const edges = (graph.edges || []).filter((edge) => nodeIds.has(edge.from) && nodeIds.has(edge.to));
    return {
      ...graph,
      nodes,
      edges,
      stats: {
        ...(graph.stats || {}),
        filtered_nodes: nodes.length,
        filtered_edges: edges.length,
      },
    };
  }

  function relationPeerName(item, currentEntityId) {
    if (!item) {
      return "—";
    }
    const fromId = Number(item.from_entity_id || 0);
    const toId = Number(item.to_entity_id || 0);
    if (currentEntityId && fromId === Number(currentEntityId)) {
      return item.to_name || item.context_title || "—";
    }
    if (currentEntityId && toId === Number(currentEntityId)) {
      return item.from_name || "—";
    }
    return item.to_name || item.from_name || "—";
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
        const isActive = selectedValue != null && String(selectedValue) === String(rowId || "");
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
        const isActive = selectedValue != null && String(selectedValue) === String(currentRowId || "");
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

  function renderLinkSection(title, items, formatter, options = {}) {
    if (!items || !items.length) {
      return "";
    }
    return `
      <div class="detail-section">
        <h3>${escapeHtml(title)}</h3>
        <ul class="detail-link-list">
          ${items
            .map((item) => {
              const jump = options.resolveJump ? options.resolveJump(item) : null;
              const secondary = options.secondary ? options.secondary(item) : "";
              const content = formatter(item);
              const body = jump?.screen && jump?.id
                ? `<button class="inline-link" data-jump-screen="${escapeHtml(jump.screen)}" data-jump-id="${escapeHtml(String(jump.id))}" type="button">${content}</button>`
                : `<div class="inline-link static">${content}</div>`;
              return `
                <li>
                  ${body}
                  ${secondary ? `<div class="link-secondary">${secondary}</div>` : ""}
                </li>
              `;
            })
            .join("")}
        </ul>
        ${options.footer ? `<div class="link-footer">${options.footer}</div>` : ""}
      </div>
    `;
  }

  function renderEvidenceGraphSection(graph, title = "Evidence graph") {
    if (!graph || !Array.isArray(graph.nodes) || graph.nodes.length < 2) {
      return "";
    }
    const layout = layoutEvidenceGraph(graph);
    if (!layout.nodes.length || !layout.edges.length) {
      return "";
    }
    return renderInteractiveGraphShell({
      title,
      hint: "Колесо: масштаб · drag: холст/ноды · click: детали",
      rootClass: "evidence-graph-shell",
      graphClass: "evidence-graph",
      mode: "evidence",
      layout,
      edgeLabelMode: "hidden",
    });
  }

  function renderBridgePathSection(paths) {
    if (!Array.isArray(paths) || !paths.length) {
      return "";
    }
    return `
      <div class="detail-section">
        <div class="graph-section-head">
          <h3>Нитки связи</h3>
          <div class="node-graph-hint">Кратчайшие объяснимые пути через claim/case/bill/contract/evidence.</div>
        </div>
        <div class="bridge-path-list">
          ${paths
            .map(
              (path) => `
                <div class="bridge-path-card">
                  <div class="bridge-path-meta">${escapeHtml(String(path.hops || 0))} hops</div>
                  <div class="bridge-path-label">${escapeHtml(path.label || "—")}</div>
                  <div class="bridge-path-steps">
                    ${(path.nodes || [])
                      .map((node) => {
                        const action = node.jump_screen && node.jump_id
                          ? `<button class="inline-link compact" data-jump-screen="${escapeHtml(node.jump_screen)}" data-jump-id="${escapeHtml(String(node.jump_id))}" type="button">${escapeHtml(node.label || node.title || "—")}</button>`
                          : `<span class="bridge-path-step static">${escapeHtml(node.label || node.title || "—")}</span>`;
                        return `<div class="bridge-path-step-wrap">${action}</div>`;
                      })
                      .join('<div class="bridge-path-arrow">→</div>')}
                  </div>
                </div>
              `
            )
            .join("")}
        </div>
      </div>
    `;
  }

  function renderRelationMapSection(graph, activeGroup = "") {
    if (!graph || !Array.isArray(graph.nodes) || graph.nodes.length < 2 || !Array.isArray(graph.edges) || !graph.edges.length) {
      return emptyState("Карта пока пуста", "Измените фильтр или дождитесь новых связей.");
    }
    const groups = relationMapGroups(graph);
    const focusedGraph = filterRelationMapGraph(graph, activeGroup);
    const layout = layoutRelationMapGraph(focusedGraph);
    if (!layout.nodes.length || !layout.edges.length) {
      return emptyState("Карта пока пуста", "Измените фильтр или дождитесь новых связей.");
    }
    const stats = focusedGraph.stats || graph.stats || {};
    return `
      <div class="relation-map-block">
        <div class="graph-section-head relation-map-head">
          <div>
            <h3>Map связей</h3>
            <div class="node-graph-hint">Большая карта текущего фильтра: общие узлы показывают, как связи переплетаются между собой. Click по линии открывает связь, click по ноде показывает полный контекст.</div>
          </div>
          <div class="relation-map-stats">
            <span class="badge cyan">${escapeHtml(String(stats.filtered_nodes || stats.nodes || layout.nodes.length))}${stats.nodes && stats.filtered_nodes && stats.filtered_nodes !== stats.nodes ? ` / ${escapeHtml(String(stats.nodes))}` : ""} nodes</span>
            <span class="badge emerald">${escapeHtml(String(stats.filtered_edges || stats.edges || layout.edges.length))}${stats.edges && stats.filtered_edges && stats.filtered_edges !== stats.edges ? ` / ${escapeHtml(String(stats.edges))}` : ""} edges</span>
            ${
              stats.bridge_nodes
                ? `<span class="badge amber">${escapeHtml(String(stats.bridge_nodes))} bridges</span>`
                : ""
            }
          </div>
        </div>
        ${
          groups.length
            ? `
              <div class="relation-map-group-row">
                <button class="filter-chip map-group-chip ${activeGroup ? "" : "active"}" data-map-group="" type="button">Все</button>
                ${groups
                  .map(
                    (group) => `
                      <button class="filter-chip map-group-chip tone-${escapeHtml(group.tone)} ${activeGroup === group.key ? "active" : ""}" data-map-group="${escapeHtml(group.key)}" type="button">
                        <span class="map-group-dot tone-${escapeHtml(group.tone)}"></span>
                        ${escapeHtml(group.label)} · ${escapeHtml(String(group.count))}
                      </button>
                    `
                  )
                  .join("")}
              </div>
            `
            : ""
        }
        ${renderInteractiveGraphShell({
          title: "",
          hint: "",
          rootClass: "relation-map-shell",
          graphClass: "relation-map-viewport",
          mode: "relation-map",
          layout,
          edgeLabelMode: "hoverless",
        })}
      </div>
    `;
  }

  function renderInteractiveGraphShell({ title, hint, rootClass, graphClass, mode, layout, edgeLabelMode = "hidden" }) {
    const nodeMap = new Map(layout.nodes.map((node) => [node.id, node]));
    return `
      <div class="detail-section evidence-graph-section ${escapeHtml(rootClass || "")}">
        ${
          title || hint
            ? `
                <div class="graph-section-head">
                  ${title ? `<h3>${escapeHtml(title)}</h3>` : "<div></div>"}
                  ${hint ? `<div class="node-graph-hint">${escapeHtml(hint)}</div>` : ""}
                </div>
              `
            : ""
        }
        <div
          class="${escapeHtml(rootClass || "evidence-graph-shell")}"
          data-graph-root
          data-graph-mode="${escapeHtml(mode || "graph")}"
          data-graph-kind="${escapeHtml(layout.kind || "graph")}"
          data-graph-width="${escapeHtml(String(layout.width || GRAPH_WIDTH))}"
          data-graph-height="${escapeHtml(String(layout.height || GRAPH_HEIGHT))}"
        >
          <div class="evidence-graph-toolbar">
            <button class="ghost-chip graph-tool-btn" data-graph-action="zoom-in" type="button">+</button>
            <button class="ghost-chip graph-tool-btn" data-graph-action="zoom-out" type="button">−</button>
            <button class="ghost-chip graph-tool-btn" data-graph-action="reset" type="button">Сброс</button>
          </div>
          <div class="${escapeHtml(graphClass || "evidence-graph")}" data-graph-viewport>
            <div class="node-graph-scene" data-graph-scene>
              <svg class="node-graph-svg" viewBox="0 0 ${layout.width || GRAPH_WIDTH} ${layout.height || GRAPH_HEIGHT}" preserveAspectRatio="none" aria-hidden="true">
                ${layout.edges
                  .map((edge) => {
                    const fromNode = nodeMap.get(edge.from);
                    const toNode = nodeMap.get(edge.to);
                    if (!fromNode || !toNode) {
                      return "";
                    }
                    const curve = buildGraphCurve(fromNode, toNode, mode);
                    const edgeKind = escapeHtml(edge.kind || "link");
                    const edgeLabel = truncate(edge.label || "", 30);
                    return `
                      <g class="node-graph-edge-group kind-${edgeKind}" data-edge-from="${escapeHtml(
                        edge.from
                      )}" data-edge-to="${escapeHtml(edge.to)}" data-edge-kind="${edgeKind}" data-edge-label="${escapeHtml(
                        edge.label || ""
                      )}" ${edge.id ? `data-edge-id="${escapeHtml(String(edge.id))}"` : ""} ${
                        edge.summary ? `data-edge-summary="${escapeHtml(edge.summary)}"` : ""
                      } ${edge.detected_label ? `data-edge-detected="${escapeHtml(edge.detected_label)}"` : ""} ${
                        edge.strength ? `data-edge-strength="${escapeHtml(edge.strength)}"` : ""
                      }>
                        <path class="node-graph-edge kind-${edgeKind}" d="${curve.path}"></path>
                        ${
                          edgeLabelMode === "visible" && edgeLabel
                            ? `<text class="node-graph-edge-label" x="${curve.label.x}" y="${curve.label.y}" text-anchor="middle">${escapeHtml(edgeLabel)}</text>`
                            : ""
                        }
                      </g>
                    `;
                  })
                  .join("")}
              </svg>
              <div class="node-graph-canvas">
                ${layout.nodes.map((node) => renderEvidenceNode(node)).join("")}
              </div>
            </div>
            <div class="node-graph-popover" data-graph-popover hidden></div>
          </div>
        </div>
      </div>
    `;
  }

  function renderEvidenceNode(node) {
    const roleClass = escapeHtml(node.role || "generic");
    const group = relationMapNodeGroup(node);
    const style = `left:${node.x}px; top:${node.y}px; --node-w:${node.width}px; --node-h:${node.height}px;`;
    return `
      <button
        class="node-graph-node role-${roleClass} group-${escapeHtml(group.tone || group.key || "other")}"
        style="${style}"
        type="button"
        data-node-id="${escapeHtml(node.id)}"
        data-node-role="${roleClass}"
        data-node-group="${escapeHtml(group.key || "other")}"
        data-node-label="${escapeHtml(node.label || "Node")}"
        data-node-title="${escapeHtml(node.title || "—")}"
        data-node-meta="${escapeHtml(node.meta || "")}"
        data-node-description="${escapeHtml(node.description || node.meta || node.title || "")}"
        data-node-x="${escapeHtml(String(node.x))}"
        data-node-y="${escapeHtml(String(node.y))}"
        data-node-width="${escapeHtml(String(node.width))}"
        data-node-height="${escapeHtml(String(node.height))}"
        ${node.jump_screen ? `data-node-jump-screen="${escapeHtml(node.jump_screen)}"` : ""}
        ${node.jump_id ? `data-node-jump-id="${escapeHtml(String(node.jump_id))}"` : ""}
      >
        <div class="node-graph-node-head">
          <span class="node-graph-node-label">${escapeHtml(node.label || "Node")}</span>
        </div>
        <div class="node-graph-node-title">${escapeHtml(node.title || "—")}</div>
        ${node.meta ? `<div class="node-graph-node-meta">${escapeHtml(node.meta)}</div>` : ""}
      </button>
    `;
  }

  function layoutEvidenceGraph(graph) {
    const nodes = (graph.nodes || []).map((node) => ({
      ...node,
      ...graphNodeDimensions(node.role),
    }));
    const edges = (graph.edges || []).filter((edge) => edge.from && edge.to);
    const layout = graph.kind === "relation"
      ? layoutRelationEvidenceGraph(nodes, edges)
      : graph.kind === "claim"
        ? layoutClaimEvidenceGraph(nodes, edges)
        : layoutGenericEvidenceGraph(nodes, edges);
    return {
      kind: graph.kind || "graph",
      width: GRAPH_WIDTH,
      height: GRAPH_HEIGHT,
      nodes: layout.nodes,
      edges: layout.edges,
    };
  }

  function layoutRelationMapGraph(graph) {
    const nodes = (graph.nodes || []).map((node) => ({
      ...node,
      ...graphNodeDimensions(node.role),
    }));
    const edges = (graph.edges || []).filter((edge) => edge.from && edge.to);
    if (!nodes.length || !edges.length) {
      return { kind: "relation_map", width: 2600, height: 1700, nodes: [], edges: [] };
    }

    const nodeMap = new Map(nodes.map((node) => [node.id, node]));
    const adjacency = new Map(nodes.map((node) => [node.id, new Set()]));
    const degree = new Map(nodes.map((node) => [node.id, 0]));
    for (const edge of edges) {
      adjacency.get(edge.from)?.add(edge.to);
      adjacency.get(edge.to)?.add(edge.from);
      degree.set(edge.from, (degree.get(edge.from) || 0) + 1);
      degree.set(edge.to, (degree.get(edge.to) || 0) + 1);
    }

    const components = [];
    const seen = new Set();
    for (const node of nodes) {
      if (seen.has(node.id)) {
        continue;
      }
      const queue = [node.id];
      const component = [];
      seen.add(node.id);
      while (queue.length) {
        const current = queue.shift();
        component.push(current);
        for (const next of adjacency.get(current) || []) {
          if (!seen.has(next)) {
            seen.add(next);
            queue.push(next);
          }
        }
      }
      components.push(component);
    }

    const columns = Math.max(1, Math.ceil(Math.sqrt(components.length)));
    const rows = Math.max(1, Math.ceil(components.length / columns));
    const componentRadii = components.map((component) => estimateRelationComponentRadius(component, nodeMap));
    const maxRadius = Math.max(...componentRadii, 620);
    const slotWidth = Math.max(1240, Math.round(maxRadius * 2.35));
    const slotHeight = Math.max(1040, Math.round(maxRadius * 2.12));
    const width = Math.max(2600, columns * slotWidth + 220);
    const height = Math.max(1700, rows * slotHeight + 220);

    components.forEach((component, index) => {
      const componentNodes = component.map((id) => nodeMap.get(id)).filter(Boolean);
      const bridgeNodes = componentNodes
        .filter((node) => String(node.role || "").startsWith("bridge_"))
        .sort((a, b) => (degree.get(b.id) || 0) - (degree.get(a.id) || 0) || a.id.localeCompare(b.id));
      const entityNodes = componentNodes
        .filter((node) => node.role === "map_entity")
        .sort((a, b) => (degree.get(b.id) || 0) - (degree.get(a.id) || 0) || a.id.localeCompare(b.id));
      const otherNodes = componentNodes.filter((node) => node.role !== "map_entity" && !String(node.role || "").startsWith("bridge_"));
      const col = index % columns;
      const row = Math.floor(index / columns);
      const centerX = 110 + slotWidth * col + slotWidth / 2;
      const centerY = 120 + row * slotHeight + slotHeight / 2;

      if (bridgeNodes.length) {
        layoutNodesInRings(bridgeNodes, centerX, centerY, {
          startRadius: bridgeNodes.length > 1 ? 146 : 0,
          ringGap: 154,
          perRing: 6,
          angleOffset: -Math.PI / 2,
        });
      } else if (entityNodes.length) {
        entityNodes[0].x = centerX;
        entityNodes[0].y = centerY;
      }

      if (!bridgeNodes.length) {
        layoutNodesInRings(entityNodes.slice(1), centerX, centerY, {
          startRadius: 330,
          ringGap: 176,
          perRing: 10,
          angleOffset: -Math.PI / 2,
        });
      } else {
        const bridgeAssignments = new Map(bridgeNodes.map((node) => [node.id, []]));
        const unassignedEntities = [];
        entityNodes.forEach((entityNode) => {
          const linkedBridgeIds = [...(adjacency.get(entityNode.id) || [])].filter((id) => bridgeAssignments.has(id));
          if (!linkedBridgeIds.length) {
            unassignedEntities.push(entityNode);
            return;
          }
          linkedBridgeIds.sort((a, b) => (degree.get(b) || 0) - (degree.get(a) || 0) || a.localeCompare(b));
          bridgeAssignments.get(linkedBridgeIds[0]).push(entityNode);
        });

        bridgeNodes.forEach((bridgeNode, bridgeIndex) => {
          const anchorAngle = bridgeNodes.length > 1 ? (bridgeIndex / bridgeNodes.length) * Math.PI * 2 : -Math.PI / 2;
          layoutNodesInRings(bridgeAssignments.get(bridgeNode.id) || [], bridgeNode.x, bridgeNode.y, {
            startRadius: 248,
            ringGap: 168,
            perRing: 7,
            angleOffset: anchorAngle,
          });
        });

        layoutNodesInRings(unassignedEntities, centerX, centerY, {
          startRadius: 520,
          ringGap: 194,
          perRing: 12,
          angleOffset: -Math.PI / 2,
        });
      }

      if (otherNodes.length) {
        layoutNodesInRings(otherNodes, centerX, centerY, {
          startRadius: 294,
          ringGap: 156,
          perRing: 8,
          angleOffset: Math.PI / 6,
        });
      }
    });

    for (const node of nodes) {
      if (node.x == null || node.y == null) {
        node.x = width / 2;
        node.y = height / 2;
      }
    }

    relaxRelationMapLayout(nodes, edges, nodeMap, width, height);
    resolveGraphNodeOverlaps(nodes, width, height, { iterations: 36, padding: 34 });
    clampGraphNodes(nodes, width, height, 42);
    const normalized = normalizeGraphBounds(nodes, 110);

    return {
      kind: "relation_map",
      width: normalized.width,
      height: normalized.height,
      nodes,
      edges,
    };
  }

  function layoutNodesInRings(nodes, centerX, centerY, options = {}) {
    if (!nodes.length) {
      return;
    }
    const startRadius = Math.max(0, options.startRadius ?? 0);
    const ringGap = Math.max(90, options.ringGap ?? 140);
    const perRing = Math.max(4, options.perRing ?? 10);
    const angleOffset = options.angleOffset ?? 0;
    nodes.forEach((node, index) => {
      if (!startRadius && index === 0 && nodes.length === 1) {
        node.x = centerX;
        node.y = centerY;
        return;
      }
      const ring = Math.floor(index / perRing);
      const slot = index % perRing;
      const nodesInRing = Math.min(perRing, Math.max(1, nodes.length - ring * perRing));
      const angle = angleOffset + (slot / nodesInRing) * Math.PI * 2;
      const radius = startRadius + ring * ringGap;
      node.x = centerX + Math.cos(angle) * radius;
      node.y = centerY + Math.sin(angle) * radius;
    });
  }

  function estimateRelationComponentRadius(component, nodeMap) {
    const componentNodes = component.map((id) => nodeMap.get(id)).filter(Boolean);
    const bridgeCount = componentNodes.filter((node) => String(node.role || "").startsWith("bridge_")).length;
    const entityCount = componentNodes.filter((node) => node.role === "map_entity").length;
    const otherCount = Math.max(0, componentNodes.length - bridgeCount - entityCount);
    const bridgeLayers = bridgeCount <= 1 ? 1 : Math.ceil(bridgeCount / 6);
    const entityLayers = bridgeCount
      ? Math.max(1, Math.ceil(entityCount / 7))
      : Math.max(1, Math.ceil(Math.max(0, entityCount - 1) / 10));
    const otherLayers = otherCount ? Math.ceil(otherCount / 8) : 0;
    return 250 + bridgeLayers * 150 + entityLayers * 172 + otherLayers * 120 + Math.sqrt(componentNodes.length) * 18;
  }

  function normalizeGraphBounds(nodes, margin = 96) {
    if (!nodes.length) {
      return { width: GRAPH_WIDTH, height: GRAPH_HEIGHT };
    }
    let minLeft = Number.POSITIVE_INFINITY;
    let minTop = Number.POSITIVE_INFINITY;
    let maxRight = Number.NEGATIVE_INFINITY;
    let maxBottom = Number.NEGATIVE_INFINITY;
    nodes.forEach((node) => {
      minLeft = Math.min(minLeft, node.x - node.width / 2);
      minTop = Math.min(minTop, node.y - node.height / 2);
      maxRight = Math.max(maxRight, node.x + node.width / 2);
      maxBottom = Math.max(maxBottom, node.y + node.height / 2);
    });
    const shiftX = margin - minLeft;
    const shiftY = margin - minTop;
    nodes.forEach((node) => {
      node.x += shiftX;
      node.y += shiftY;
    });
    return {
      width: Math.max(960, Math.ceil(maxRight - minLeft + margin * 2)),
      height: Math.max(680, Math.ceil(maxBottom - minTop + margin * 2)),
    };
  }

  function relaxRelationMapLayout(nodes, edges, nodeMap, width, height) {
    const edgePairs = edges
      .map((edge) => [nodeMap.get(edge.from), nodeMap.get(edge.to), edge])
      .filter(([fromNode, toNode]) => fromNode && toNode);
    if (!nodes.length) {
      return;
    }

    for (let iteration = 0; iteration < 34; iteration += 1) {
      const forces = new Map(nodes.map((node) => [node.id, { x: 0, y: 0 }]));

      for (let i = 0; i < nodes.length; i += 1) {
        const a = nodes[i];
        for (let j = i + 1; j < nodes.length; j += 1) {
          const b = nodes[j];
          let dx = b.x - a.x;
          let dy = b.y - a.y;
          let dist = Math.hypot(dx, dy);
          if (dist < 0.001) {
            const angle = ((i + j + 1) % 12) * (Math.PI / 6);
            dx = Math.cos(angle);
            dy = Math.sin(angle);
            dist = 1;
          }
          const minDist = (a.width + b.width) / 2 + 34;
          if (dist > minDist * 1.6) {
            continue;
          }
          const force = ((minDist * 1.45 - dist) / (minDist * 1.45)) * 5.4;
          const fx = (dx / dist) * force;
          const fy = (dy / dist) * force;
          forces.get(a.id).x -= fx;
          forces.get(a.id).y -= fy;
          forces.get(b.id).x += fx;
          forces.get(b.id).y += fy;
        }
      }

      edgePairs.forEach(([fromNode, toNode, edge]) => {
        const dx = toNode.x - fromNode.x;
        const dy = toNode.y - fromNode.y;
        const dist = Math.max(1, Math.hypot(dx, dy));
        const ideal = String(edge.kind || "").startsWith("weak") ? 250 : 220;
        const pull = (dist - ideal) * 0.011;
        const fx = (dx / dist) * pull;
        const fy = (dy / dist) * pull;
        forces.get(fromNode.id).x += fx;
        forces.get(fromNode.id).y += fy;
        forces.get(toNode.id).x -= fx;
        forces.get(toNode.id).y -= fy;
      });

      nodes.forEach((node) => {
        const nodeForce = forces.get(node.id);
        const damping = String(node.role || "").startsWith("bridge_") ? 0.48 : 0.82;
        node.x += nodeForce.x * damping;
        node.y += nodeForce.y * damping;
      });
      clampGraphNodes(nodes, width, height, 42);
    }
  }

  function resolveGraphNodeOverlaps(nodes, width, height, options = {}) {
    const iterations = Math.max(1, options.iterations ?? 18);
    const padding = Math.max(12, options.padding ?? 18);
    for (let step = 0; step < iterations; step += 1) {
      let moved = false;
      for (let i = 0; i < nodes.length; i += 1) {
        const a = nodes[i];
        for (let j = i + 1; j < nodes.length; j += 1) {
          const b = nodes[j];
          const minX = (a.width + b.width) / 2 + padding;
          const minY = (a.height + b.height) / 2 + padding;
          let dx = b.x - a.x;
          let dy = b.y - a.y;
          if (Math.abs(dx) >= minX || Math.abs(dy) >= minY) {
            continue;
          }
          if (Math.abs(dx) < 0.001 && Math.abs(dy) < 0.001) {
            const angle = ((i + j + 3) % 12) * (Math.PI / 6);
            dx = Math.cos(angle);
            dy = Math.sin(angle);
          }
          const overlapX = minX - Math.abs(dx);
          const overlapY = minY - Math.abs(dy);
          if (overlapX <= 0 || overlapY <= 0) {
            continue;
          }
          moved = true;
          if (overlapX < overlapY) {
            const push = overlapX / 2 + 1;
            const direction = dx >= 0 ? 1 : -1;
            a.x -= push * direction;
            b.x += push * direction;
          } else {
            const push = overlapY / 2 + 1;
            const direction = dy >= 0 ? 1 : -1;
            a.y -= push * direction;
            b.y += push * direction;
          }
        }
      }
      clampGraphNodes(nodes, width, height, 42);
      if (!moved) {
        break;
      }
    }
  }

  function clampGraphNodes(nodes, width, height, margin = 24) {
    nodes.forEach((node) => {
      node.x = Math.max(node.width / 2 + margin, Math.min(width - node.width / 2 - margin, node.x));
      node.y = Math.max(node.height / 2 + margin, Math.min(height - node.height / 2 - margin, node.y));
    });
  }

  function layoutClaimEvidenceGraph(nodes, edges) {
    const claim = nodes.find((node) => node.role === "claim") || nodes[0];
    const caseNodes = nodes.filter((node) => node.role === "case");
    const contentNodes = nodes.filter((node) => node.role === "content_origin" || node.role === "content");
    const entityNodes = nodes.filter((node) => node.role === "entity");
    const evidenceNodes = nodes.filter((node) => node.role === "evidence");
    const assigned = new Set(
      [claim, ...caseNodes, ...contentNodes, ...entityNodes, ...evidenceNodes]
        .filter(Boolean)
        .map((node) => node.id)
    );
    const otherNodes = nodes.filter((node) => !assigned.has(node.id));

    if (claim) {
      claim.x = 500;
      claim.y = 318;
    }
    stackGraphNodes(caseNodes, 500, 112, 112);
    stackGraphNodes([...contentNodes, ...entityNodes], 190, 332, 124);
    stackGraphNodes(evidenceNodes, 810, 332, 124);
    stackGraphNodes(otherNodes, 500, 520, 110);
    return { nodes, edges };
  }

  function layoutRelationEvidenceGraph(nodes, edges) {
    const relation = nodes.find((node) => node.role === "relation") || nodes[0];
    const leftNodes = nodes.filter((node) => node.role === "entity_from");
    const rightNodes = nodes.filter((node) => node.role === "entity_to");
    const contextNodes = nodes.filter((node) => node.role === "context");
    const evidenceNodes = nodes.filter((node) => node.role === "evidence");
    const assigned = new Set(
      [relation, ...leftNodes, ...rightNodes, ...contextNodes, ...evidenceNodes]
        .filter(Boolean)
        .map((node) => node.id)
    );
    const otherNodes = nodes.filter((node) => !assigned.has(node.id));

    if (relation) {
      relation.x = 500;
      relation.y = 308;
    }
    stackGraphNodes(leftNodes, 190, 308, 124);
    stackGraphNodes(rightNodes, 810, 308, 124);
    stackGraphNodes(contextNodes, 500, 116, 112);
    stackGraphNodes(evidenceNodes, 500, 516, 112);
    stackGraphNodes(otherNodes, 810, 516, 108);
    return { nodes, edges };
  }

  function layoutGenericEvidenceGraph(nodes, edges) {
    const columns = [170, 500, 830];
    const groups = [[], [], []];
    nodes.forEach((node, index) => {
      groups[index % columns.length].push(node);
    });
    groups.forEach((group, index) => {
      stackGraphNodes(group, columns[index], 310, 122);
    });
    return { nodes, edges };
  }

  function graphNodeDimensions(role) {
    switch (role) {
      case "claim":
        return { width: 286, height: 120 };
      case "relation":
        return { width: 252, height: 112 };
      case "bridge_claim":
        return { width: 272, height: 110 };
      case "bridge_case":
      case "bridge_bill":
      case "bridge_contract":
      case "bridge_affiliation":
      case "bridge_restriction":
        return { width: 244, height: 102 };
      case "bridge_content":
      case "bridge_evidence":
        return { width: 236, height: 100 };
      case "context":
      case "case":
        return { width: 228, height: 98 };
      case "content_origin":
        return { width: 236, height: 102 };
      case "map_entity":
        return { width: 250, height: 108 };
      default:
        return { width: 220, height: 96 };
    }
  }

  function stackGraphNodes(nodes, x, centerY, gap) {
    if (!nodes.length) {
      return;
    }
    const totalHeight = gap * (nodes.length - 1);
    const startY = centerY - totalHeight / 2;
    nodes.forEach((node, index) => {
      node.x = x;
      node.y = startY + index * gap;
    });
  }

  function buildEvidenceCurve(source, target) {
    const [fromSide, toSide] = graphEdgePorts(source, target);
    const start = graphNodePortPoint(source, fromSide);
    const end = graphNodePortPoint(target, toSide);
    const horizontal = fromSide === "left" || fromSide === "right";
    const primaryDistance = horizontal ? Math.abs(end.x - start.x) : Math.abs(end.y - start.y);
    const secondaryDistance = horizontal ? Math.abs(end.y - start.y) : Math.abs(end.x - start.x);
    const bendCap = Math.max(8, primaryDistance / 2 - 6);
    const preferredBend = primaryDistance * 0.42 + secondaryDistance * 0.06;
    const bend = Math.max(8, Math.min(140, preferredBend, bendCap));
    let cp1;
    let cp2;
    if (horizontal) {
      const direction = start.x <= end.x ? 1 : -1;
      cp1 = { x: Math.round(start.x + bend * direction), y: Math.round(start.y) };
      cp2 = { x: Math.round(end.x - bend * direction), y: Math.round(end.y) };
    } else {
      const direction = start.y <= end.y ? 1 : -1;
      cp1 = { x: Math.round(start.x), y: Math.round(start.y + bend * direction) };
      cp2 = { x: Math.round(end.x), y: Math.round(end.y - bend * direction) };
    }
    return {
      start,
      end,
      label: {
        x: Math.round((start.x + end.x) / 2),
        y: Math.round((start.y + end.y) / 2 - 10),
      },
      path: `M ${start.x} ${start.y} C ${cp1.x} ${cp1.y}, ${cp2.x} ${cp2.y}, ${end.x} ${end.y}`,
    };
  }

  function buildRelationMapCurve(source, target) {
    const sourceOnLeft = source.x <= target.x;
    const start = graphNodePortPoint(source, sourceOnLeft ? "right" : "left");
    const end = graphNodePortPoint(target, sourceOnLeft ? "left" : "right");
    const direction = sourceOnLeft ? 1 : -1;
    const horizontalDistance = Math.max(64, Math.abs(end.x - start.x));
    const bend = Math.max(42, Math.min(160, horizontalDistance * 0.46));
    const cp1 = { x: Math.round(start.x + bend * direction), y: Math.round(start.y) };
    const cp2 = { x: Math.round(end.x - bend * direction), y: Math.round(end.y) };
    return {
      start,
      end,
      label: {
        x: Math.round((start.x + end.x) / 2),
        y: Math.round((start.y + end.y) / 2 - 12),
      },
      path: `M ${start.x} ${start.y} C ${cp1.x} ${cp1.y}, ${cp2.x} ${cp2.y}, ${end.x} ${end.y}`,
    };
  }

  function buildGraphCurve(source, target, mode) {
    if (mode === "relation-map") {
      return buildRelationMapCurve(source, target);
    }
    return buildEvidenceCurve(source, target);
  }

  function graphEdgePorts(source, target) {
    const dx = target.x - source.x;
    const dy = target.y - source.y;
    if (Math.abs(dx) >= Math.abs(dy) * 0.75) {
      return dx >= 0 ? ["right", "left"] : ["left", "right"];
    }
    return dy >= 0 ? ["bottom", "top"] : ["top", "bottom"];
  }

  function graphNodePortPoint(node, side) {
    const halfWidth = node.width / 2;
    const halfHeight = node.height / 2;
    if (side === "left") {
      return { x: Math.round(node.x - halfWidth), y: Math.round(node.y) };
    }
    if (side === "right") {
      return { x: Math.round(node.x + halfWidth), y: Math.round(node.y) };
    }
    if (side === "top") {
      return { x: Math.round(node.x), y: Math.round(node.y - halfHeight) };
    }
    return { x: Math.round(node.x), y: Math.round(node.y + halfHeight) };
  }

  function graphControlPoint(point, side, bend) {
    if (side === "left") {
      return { x: Math.round(point.x - bend), y: point.y };
    }
    if (side === "right") {
      return { x: Math.round(point.x + bend), y: point.y };
    }
    if (side === "top") {
      return { x: point.x, y: Math.round(point.y - bend) };
    }
    return { x: point.x, y: Math.round(point.y + bend) };
  }

  function bindEvidenceGraphs() {
    if (relationMapOverlayOpen()) {
      syncRelationMapStage();
    }
    queryInteractiveAll("[data-graph-root]").forEach((graphRoot) => {
      initializeEvidenceGraph(graphRoot);
    });
    if (relationMapOverlayOpen()) {
      requestAnimationFrame(() => {
        syncRelationMapStage();
        refitVisibleGraphs();
      });
    }
  }

  function syncRelationMapStage() {
    const host = ui.relationMapOverlayHost;
    const overlay = host?.querySelector(".relation-map-screen");
    if (!host || !overlay || host.hidden) {
      return;
    }
    const screenPanelRect = ui.appShell.querySelector(".screen-panel")?.getBoundingClientRect();
    if (!screenPanelRect) {
      return;
    }
    const left = Math.round(screenPanelRect.left);
    const top = Math.round(screenPanelRect.top);
    const width = Math.max(760, Math.round(screenPanelRect.width));
    const height = Math.max(620, Math.round(screenPanelRect.height));
    host.style.left = `${left}px`;
    host.style.top = `${top}px`;
    host.style.width = `${width}px`;
    host.style.height = `${height}px`;
  }

  function refitVisibleGraphs() {
    queryInteractiveAll("[data-graph-root]").forEach((graphRoot) => {
      const graph = graphRoot.__graphState;
      if (!graph) {
        return;
      }
      if (graph.mode === "relation-map") {
        resetGraphViewport(graph);
        updateGraphEdges(graph);
      }
    });
  }

  function initializeEvidenceGraph(graphRoot) {
    if (!graphRoot || graphRoot.dataset.graphBound === "true") {
      return;
    }
    graphRoot.dataset.graphBound = "true";

    const viewport = graphRoot.querySelector("[data-graph-viewport]");
    const scene = graphRoot.querySelector("[data-graph-scene]");
    const popover = graphRoot.querySelector("[data-graph-popover]");
    if (!viewport || !scene || !popover) {
      return;
    }

    const graph = {
      root: graphRoot,
      viewport,
      scene,
      popover,
      mode: graphRoot.dataset.graphMode || "graph",
      width: Number(graphRoot.dataset.graphWidth || GRAPH_WIDTH),
      height: Number(graphRoot.dataset.graphHeight || GRAPH_HEIGHT),
      scale: 1,
      panX: 0,
      panY: 0,
      dragNode: null,
      panning: null,
      moved: false,
      nodes: new Map(),
      edges: [],
    };
    graphRoot.__graphState = graph;

    scene.style.width = `${graph.width}px`;
    scene.style.height = `${graph.height}px`;
    scene.querySelectorAll(".node-graph-svg, .node-graph-canvas").forEach((el) => {
      el.style.width = `${graph.width}px`;
      el.style.height = `${graph.height}px`;
    });

    scene.querySelectorAll(".node-graph-node").forEach((nodeEl) => {
      const node = {
        el: nodeEl,
        id: nodeEl.dataset.nodeId,
        role: nodeEl.dataset.nodeRole || "node",
        label: nodeEl.dataset.nodeLabel || "Node",
        title: nodeEl.dataset.nodeTitle || "—",
        meta: nodeEl.dataset.nodeMeta || "",
        description: nodeEl.dataset.nodeDescription || "",
        jumpScreen: nodeEl.dataset.nodeJumpScreen || "",
        jumpId: nodeEl.dataset.nodeJumpId || "",
        x: Number(nodeEl.dataset.nodeX || 0),
        y: Number(nodeEl.dataset.nodeY || 0),
        width: Number(nodeEl.dataset.nodeWidth || 220),
        height: Number(nodeEl.dataset.nodeHeight || 96),
      };
      graph.nodes.set(node.id, node);
      updateGraphNodePosition(node);
      nodeEl.addEventListener("pointerdown", (event) => startNodeDrag(graph, node, event));
      nodeEl.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        if (!graph.dragNode) {
          openGraphPopover(graph, node);
        }
      });
    });

    scene.querySelectorAll(".node-graph-edge-group").forEach((edgeEl) => {
      const rawEdgeId = edgeEl.dataset.edgeId || "";
      const edge = {
        id: /^\d+$/.test(rawEdgeId) ? Number(rawEdgeId) : rawEdgeId,
        from: edgeEl.dataset.edgeFrom || "",
        to: edgeEl.dataset.edgeTo || "",
        kind: edgeEl.dataset.edgeKind || "link",
        label: edgeEl.dataset.edgeLabel || "",
        summary: edgeEl.dataset.edgeSummary || "",
        detectedLabel: edgeEl.dataset.edgeDetected || "",
        strength: edgeEl.dataset.edgeStrength || "",
        groupEl: edgeEl,
        pathEl: edgeEl.querySelector(".node-graph-edge"),
        labelEl: edgeEl.querySelector(".node-graph-edge-label"),
      };
      graph.edges.push(edge);
      if (graph.mode === "relation-map" && edge.id) {
        edgeEl.classList.add("interactive");
        edgeEl.addEventListener("click", async (event) => {
          event.preventDefault();
          event.stopPropagation();
          if (edge.id) {
            state.selectedRows.relations = edge.id;
            openDetailDrawer("relations");
            await loadCurrentScreen();
          }
        });
      }
    });

      graphRoot.querySelectorAll("[data-graph-action]").forEach((button) => {
      button.addEventListener("click", (event) => {
        event.preventDefault();
        const action = button.dataset.graphAction;
        if (action === "zoom-in") {
          setGraphZoom(graph, graph.scale * 1.12, viewport.clientWidth / 2, viewport.clientHeight / 2);
        } else if (action === "zoom-out") {
          setGraphZoom(graph, graph.scale / 1.12, viewport.clientWidth / 2, viewport.clientHeight / 2);
        } else {
          resetGraphViewport(graph);
        }
      });
    });

    viewport.addEventListener(
      "wheel",
      (event) => {
        event.preventDefault();
        const rect = viewport.getBoundingClientRect();
        const nextScale = event.deltaY < 0 ? graph.scale * 1.06 : graph.scale / 1.06;
        setGraphZoom(graph, nextScale, event.clientX - rect.left, event.clientY - rect.top);
      },
      { passive: false }
    );

    viewport.addEventListener("pointerdown", (event) => {
      if (event.target.closest(".node-graph-node, .node-graph-popover")) {
        return;
      }
      closeGraphPopover(graph);
      startGraphPan(graph, event);
    });

    viewport.addEventListener("dblclick", () => {
      resetGraphViewport(graph);
    });

    resetGraphViewport(graph);
    updateGraphEdges(graph);
  }

  function startNodeDrag(graph, node, event) {
    event.preventDefault();
    event.stopPropagation();
    closeGraphPopover(graph);

    const pointerId = event.pointerId;
    const startX = event.clientX;
    const startY = event.clientY;
    const baseX = node.x;
    const baseY = node.y;
    graph.dragNode = node;
    graph.moved = false;
    node.el.classList.add("dragging");
    node.el.setPointerCapture(pointerId);

    const onMove = (moveEvent) => {
      if (moveEvent.pointerId !== pointerId) {
        return;
      }
      const deltaX = (moveEvent.clientX - startX) / graph.scale;
      const deltaY = (moveEvent.clientY - startY) / graph.scale;
      if (Math.abs(deltaX) > 2 || Math.abs(deltaY) > 2) {
        graph.moved = true;
      }
      node.x = Math.max(node.width / 2, Math.min(graph.width - node.width / 2, baseX + deltaX));
      node.y = Math.max(node.height / 2, Math.min(graph.height - node.height / 2, baseY + deltaY));
      resolveLocalNodeCollisions(graph, node);
      updateGraphNodePosition(node);
      updateGraphEdges(graph);
    };

    const onUp = (upEvent) => {
      if (upEvent.pointerId !== pointerId) {
        return;
      }
      node.el.releasePointerCapture(pointerId);
      node.el.classList.remove("dragging");
      node.el.removeEventListener("pointermove", onMove);
      node.el.removeEventListener("pointerup", onUp);
      node.el.removeEventListener("pointercancel", onUp);
      graph.dragNode = null;
      if (!graph.moved) {
        openGraphPopover(graph, node);
      }
    };

    node.el.addEventListener("pointermove", onMove);
    node.el.addEventListener("pointerup", onUp);
    node.el.addEventListener("pointercancel", onUp);
  }

  function startGraphPan(graph, event) {
    const pointerId = event.pointerId;
    const startX = event.clientX;
    const startY = event.clientY;
    const baseX = graph.panX;
    const baseY = graph.panY;
    graph.panning = { pointerId };
    graph.viewport.classList.add("panning");
    graph.viewport.setPointerCapture(pointerId);

    const onMove = (moveEvent) => {
      if (moveEvent.pointerId !== pointerId) {
        return;
      }
      graph.panX = baseX + (moveEvent.clientX - startX);
      graph.panY = baseY + (moveEvent.clientY - startY);
      applyGraphTransform(graph);
    };

    const onUp = (upEvent) => {
      if (upEvent.pointerId !== pointerId) {
        return;
      }
      graph.viewport.releasePointerCapture(pointerId);
      graph.viewport.classList.remove("panning");
      graph.viewport.removeEventListener("pointermove", onMove);
      graph.viewport.removeEventListener("pointerup", onUp);
      graph.viewport.removeEventListener("pointercancel", onUp);
      graph.panning = null;
    };

    graph.viewport.addEventListener("pointermove", onMove);
    graph.viewport.addEventListener("pointerup", onUp);
    graph.viewport.addEventListener("pointercancel", onUp);
  }

  function updateGraphNodePosition(node) {
    node.el.style.left = `${node.x}px`;
    node.el.style.top = `${node.y}px`;
    node.el.dataset.nodeX = String(node.x);
    node.el.dataset.nodeY = String(node.y);
  }

  function updateGraphEdges(graph) {
    graph.edges.forEach((edge) => {
      const fromNode = graph.nodes.get(edge.from);
      const toNode = graph.nodes.get(edge.to);
      if (!fromNode || !toNode || !edge.pathEl) {
        return;
      }
      const curve = buildGraphCurve(fromNode, toNode, graph.mode);
      edge.pathEl.setAttribute("d", curve.path);
      if (edge.labelEl) {
        edge.labelEl.setAttribute("x", String(curve.label.x));
        edge.labelEl.setAttribute("y", String(curve.label.y));
      }
    });
  }

  function resetGraphViewport(graph) {
    const bounds = computeGraphContentBounds([...graph.nodes.values()], graph.mode === "relation-map" ? 96 : 48);
    const targetWidth = Math.max(1, bounds.width);
    const targetHeight = Math.max(1, bounds.height);
    const fitScale = Math.min(
      1,
      (graph.viewport.clientWidth - 36) / targetWidth,
      (graph.viewport.clientHeight - 36) / targetHeight
    );
    const minScale = graph.mode === "relation-map" ? 0.04 : 0.48;
    const maxScale = graph.mode === "relation-map" ? 1.8 : 1.95;
    graph.scale = Math.max(minScale, fitScale);
    graph.scale = Math.min(graph.scale, maxScale);
    graph.panX = Math.round(graph.viewport.clientWidth / 2 - bounds.centerX * graph.scale);
    graph.panY = Math.round(graph.viewport.clientHeight / 2 - bounds.centerY * graph.scale);
    applyGraphTransform(graph);
    closeGraphPopover(graph);
  }

  function setGraphZoom(graph, nextScale, anchorX, anchorY) {
    const minScale = graph.mode === "relation-map" ? 0.04 : 0.48;
    const maxScale = graph.mode === "relation-map" ? 1.8 : 1.95;
    const clamped = Math.max(minScale, Math.min(maxScale, nextScale));
    const sceneX = (anchorX - graph.panX) / graph.scale;
    const sceneY = (anchorY - graph.panY) / graph.scale;
    graph.scale = clamped;
    graph.panX = anchorX - sceneX * graph.scale;
    graph.panY = anchorY - sceneY * graph.scale;
    applyGraphTransform(graph);
    closeGraphPopover(graph);
  }

  function applyGraphTransform(graph) {
    graph.scene.style.transform = `translate(${graph.panX}px, ${graph.panY}px) scale(${graph.scale})`;
  }

  function openGraphPopover(graph, node) {
    if (!graph.popover || !node) {
      return;
    }
    const action = node.jumpScreen && node.jumpId
      ? `<button class="inline-link compact" data-jump-screen="${escapeHtml(node.jumpScreen)}" data-jump-id="${escapeHtml(
          String(node.jumpId)
        )}" type="button">Открыть карточку</button>`
      : "";
    graph.popover.innerHTML = `
      <button class="graph-popover-close" data-graph-popover-close type="button">×</button>
      <div class="eyebrow">${escapeHtml(node.label || node.role || "Node")}</div>
      <div class="graph-popover-title">${escapeHtml(node.title || "—")}</div>
      ${node.meta ? `<div class="graph-popover-meta">${escapeHtml(node.meta)}</div>` : ""}
      <div class="graph-popover-body">${escapeHtml(node.description || node.title || "—")}</div>
      ${action ? `<div class="graph-popover-actions">${action}</div>` : ""}
    `;
    graph.popover.hidden = false;
    graph.popover.classList.add("open");
    graph.popover.scrollTop = 0;
    const nodeScreenX = node.x * graph.scale + graph.panX;
    const nodeScreenY = node.y * graph.scale + graph.panY;
    const nodeOffsetX = (node.width * graph.scale) / 2 + 20;
    const popoverWidth = graph.popover.offsetWidth || 420;
    const popoverHeight = graph.popover.offsetHeight || 320;
    const viewportWidth = graph.viewport.clientWidth;
    const viewportHeight = graph.viewport.clientHeight;
    const preferredLeft = nodeScreenX + nodeOffsetX;
    const fallbackLeft = nodeScreenX - nodeOffsetX - popoverWidth;
    const left = preferredLeft + popoverWidth <= viewportWidth - 18 ? preferredLeft : fallbackLeft;
    const top = nodeScreenY - popoverHeight / 2;
    graph.popover.style.left = `${Math.max(18, Math.min(viewportWidth - popoverWidth - 18, left))}px`;
    graph.popover.style.top = `${Math.max(18, Math.min(viewportHeight - popoverHeight - 18, top))}px`;
    graph.popover.querySelectorAll("[data-jump-screen]").forEach((button) => {
      button.addEventListener("click", async (event) => {
        event.preventDefault();
        event.stopPropagation();
        await navigateTo(button.dataset.jumpScreen, Number(button.dataset.jumpId));
      });
    });
    graph.popover.querySelectorAll("[data-graph-popover-close]").forEach((button) => {
      button.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        closeGraphPopover(graph);
      });
    });
  }

  function closeGraphPopover(graph) {
    if (!graph?.popover) {
      return;
    }
    graph.popover.hidden = true;
    graph.popover.classList.remove("open");
    graph.popover.innerHTML = "";
  }

  function closeVisibleGraphPopover() {
    let closed = false;
    queryInteractiveAll("[data-graph-root]").forEach((graphRoot) => {
      const graph = graphRoot.__graphState;
      if (graph?.popover && !graph.popover.hidden) {
        closeGraphPopover(graph);
        closed = true;
      }
    });
    return closed;
  }

  function resolveLocalNodeCollisions(graph, pivotNode) {
    const padding = graph.mode === "relation-map" ? 30 : 18;
    graph.nodes.forEach((otherNode) => {
      if (!otherNode || otherNode.id === pivotNode.id) {
        return;
      }
      const minX = (pivotNode.width + otherNode.width) / 2 + padding;
      const minY = (pivotNode.height + otherNode.height) / 2 + padding;
      let dx = otherNode.x - pivotNode.x;
      let dy = otherNode.y - pivotNode.y;
      if (Math.abs(dx) >= minX || Math.abs(dy) >= minY) {
        return;
      }
      if (Math.abs(dx) < 0.001 && Math.abs(dy) < 0.001) {
        dx = 1;
        dy = 0;
      }
      const overlapX = minX - Math.abs(dx);
      const overlapY = minY - Math.abs(dy);
      if (overlapX <= 0 || overlapY <= 0) {
        return;
      }
      if (overlapX < overlapY) {
        otherNode.x += (dx >= 0 ? 1 : -1) * (overlapX + 2);
      } else {
        otherNode.y += (dy >= 0 ? 1 : -1) * (overlapY + 2);
      }
      otherNode.x = Math.max(otherNode.width / 2, Math.min(graph.width - otherNode.width / 2, otherNode.x));
      otherNode.y = Math.max(otherNode.height / 2, Math.min(graph.height - otherNode.height / 2, otherNode.y));
      updateGraphNodePosition(otherNode);
    });
  }

  function computeGraphContentBounds(nodes, padding = 48) {
    if (!nodes.length) {
      return { left: 0, top: 0, right: GRAPH_WIDTH, bottom: GRAPH_HEIGHT, width: GRAPH_WIDTH, height: GRAPH_HEIGHT, centerX: GRAPH_WIDTH / 2, centerY: GRAPH_HEIGHT / 2 };
    }
    let left = Number.POSITIVE_INFINITY;
    let top = Number.POSITIVE_INFINITY;
    let right = Number.NEGATIVE_INFINITY;
    let bottom = Number.NEGATIVE_INFINITY;
    nodes.forEach((node) => {
      left = Math.min(left, node.x - node.width / 2);
      top = Math.min(top, node.y - node.height / 2);
      right = Math.max(right, node.x + node.width / 2);
      bottom = Math.max(bottom, node.y + node.height / 2);
    });
    left -= padding;
    top -= padding;
    right += padding;
    bottom += padding;
    return {
      left,
      top,
      right,
      bottom,
      width: right - left,
      height: bottom - top,
      centerX: (left + right) / 2,
      centerY: (top + bottom) / 2,
    };
  }

  function bindDetailDrawer() {
    queryInteractiveAll("[data-detail-close]").forEach((button) => {
      button.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        closeDetailDrawer(state.section);
        delete state.selectedRows[state.section];
        renderScreen();
      });
    });
  }

  function bindTextFilter(section) {
    const input = queryInteractive("#screen-query-input");
    if (!input) {
      return;
    }
    input.addEventListener("input", (event) => {
      state.filters[section].query = event.target.value;
      scheduleScreenReload();
    });
  }

  function bindRowSelection(section, idField) {
    queryInteractiveAll("[data-row-id]").forEach((row) => {
      row.addEventListener("click", async () => {
        state.selectedRows[section] = Number(row.dataset.rowId);
        if (idField === "entity_id") {
          state.selectedRows[section] = Number(row.dataset.rowId);
        }
        openDetailDrawer(section);
        await loadCurrentScreen();
      });
    });
  }

  function bindViewSwitch(section) {
    queryInteractiveAll("[data-screen-view]").forEach((button) => {
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

  function bindJumpLinks() {
    queryInteractiveAll("[data-jump-screen][data-jump-id]").forEach((node) => {
      node.addEventListener("click", async (event) => {
        event.preventDefault();
        event.stopPropagation();
        await navigateTo(node.dataset.jumpScreen, Number(node.dataset.jumpId));
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

  async function navigateTo(screen, id) {
    if (!screen || !id) {
      return;
    }
    state.group = groupKeyForSection(screen);
    state.section = screen;
    state.selectedRows[screen] = Number(id);
    openDetailDrawer(screen);
    renderShell();
    await loadCurrentScreen();
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
      events: "Канонические события с нарративом, ролями участников, таймлайном и supporting docs.",
      review_ops: "Очереди ручной и полуавтоматической верификации, merge и promotion.",
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
          { key: "verification", label: "Проверка", sections: [{ key: "claims", label: "Заявления" }, { key: "cases", label: "Дела" }, { key: "review_ops", label: "Review Ops" }] },
          { key: "analytics", label: "Аналитика", sections: [{ key: "events", label: "События" }, { key: "entities", label: "Сущности" }, { key: "relations", label: "Связи" }, { key: "officials", label: "Руководство" }] },
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
          detail: {
            id: 21,
            claim_text: "Иванов Иван Иванович занимает должность министра",
            status: "verified",
            content_id: 11,
            content_title: "Executive profile snapshot",
            source_name: "Минфин",
            published_at: "2026-04-25",
            case_id: 31,
            case_title: "Кейс назначения",
            evidence: [{ id: 91, evidence_type: "official_profile", evidence_title: "Executive profile snapshot", evidence_item_id: 11 }],
            evidence_graph: {
              kind: "claim",
              nodes: [
                { id: "claim:21", role: "claim", label: "Claim", title: "Иванов Иван Иванович занимает должность министра", meta: "verified", description: "Основной claim о назначении чиновника.", jump_screen: "claims", jump_id: 21 },
                { id: "content:11", role: "content_origin", label: "Источник", title: "Executive profile snapshot", meta: "Минфин · 2026-04-25", description: "Профиль руководителя на официальном сайте.", jump_screen: "content", jump_id: 11 },
                { id: "entity:1", role: "entity", label: "person", title: "Иванов Иван Иванович", meta: "subject", description: "Персона, фигурирующая в публикации и в claim.", jump_screen: "entities", jump_id: 1 },
                { id: "case:31", role: "case", label: "Дело", title: "Кейс назначения", meta: "oversight", description: "Сводный кейс по назначению и подтверждающим публикациям.", jump_screen: "cases", jump_id: 31 },
                { id: "evidence:91", role: "evidence", label: "Evidence", title: "Executive profile snapshot", meta: "official_profile", description: "Профиль используется как evidence для claim.", jump_screen: "content", jump_id: 11 },
              ],
              edges: [
                { from: "content:11", to: "claim:21", label: "источник", kind: "origin" },
                { from: "entity:1", to: "claim:21", label: "subject", kind: "entity" },
                { from: "case:31", to: "claim:21", label: "в деле", kind: "case" },
                { from: "evidence:91", to: "claim:21", label: "official_profile", kind: "evidence" },
              ],
            },
          },
        },
        cases: {
          items: [{ id: 31, title: "Кейс назначения", status: "open", case_type: "oversight", claims_count: 3 }],
          detail: { id: 31, title: "Кейс назначения", status: "open", case_type: "oversight", claims: [{ claim_text: "Иванов занимает должность" }], events: [{ event_date: "2026-04-25", event_title: "Публикация профиля" }] },
        },
        events: {
          items: [
            {
              id: 61,
              canonical_title: "Блокировка Telegram",
              event_type: "internet_block",
              status: "active",
              event_date_start: "2026-04-20",
              event_date_end: "2026-04-21",
              importance_score: 0.92,
              summary_short: "Регулятор инициировал ограничение доступа, после чего начались жалобы пользователей и официальные разъяснения.",
            },
          ],
          detail: {
            id: 61,
            canonical_title: "Блокировка Telegram",
            event_type: "internet_block",
            status: "active",
            event_date_start: "2026-04-20",
            event_date_end: "2026-04-21",
            importance_score: 0.92,
            summary_short: "Регулятор инициировал ограничение доступа, после чего начались жалобы пользователей и официальные разъяснения.",
            summary_long: "Событие объединяет официальное решение об ограничении доступа, первые сообщения о практической блокировке, жалобы пользователей и публичные комментарии ведомств.",
            timeline: [
              { timeline_date: "2026-04-20", title: "Постановление опубликовано", description: "Опубликован официальный документ об ограничении доступа.", content_item_id: 11 },
              { timeline_date: "2026-04-21", title: "Пошли жалобы пользователей", description: "Пользователи сообщают о фактических сбоях доступа к Telegram.", content_item_id: 12 },
            ],
            entities: [
              { entity_id: 4, canonical_name: "Роскомнадзор", entity_type: "organization", role: "regulator", valid_from: "2026-04-20" },
              { entity_id: 2, canonical_name: "Telegram", entity_type: "organization", role: "target", valid_from: "2026-04-20" },
            ],
            facts: [
              { id: 81, fact_type: "restriction", canonical_text: "Ведомство ограничило доступ к Telegram.", polarity: "negative", valid_from: "2026-04-20" },
              { id: 82, fact_type: "statement", canonical_text: "Пользователи сообщили о сбоях доступа.", polarity: "negative", valid_from: "2026-04-21" },
            ],
            items: [
              { content_item_id: 11, title: "Постановление об ограничении Telegram", item_role: "official_doc", source_name: "РКН", published_at: "2026-04-20", source_strength: "hard" },
              { content_item_id: 12, title: "Жалобы пользователей на сбои Telegram", item_role: "update", source_name: "СМИ", published_at: "2026-04-21", source_strength: "support" },
            ],
          },
        },
        review_ops: {
          queues: [{ queue_key: "content_duplicates", total: 1, open_total: 1 }],
          items: [{ id: 501, queue_key: "content_duplicates", subject_type: "content_cluster", suggested_action: "merge", confidence: 0.91, status: "open", machine_reason: "Normalized duplicate" }],
          detail: { id: 501, queue_key: "content_duplicates", subject_type: "content_cluster", suggested_action: "merge", confidence: 0.91, status: "open", subject_summary: "dup cluster · items 2", candidate_payload_pretty: "{\n  \"items\": [11, 12]\n}" },
        },
        entities: {
          items: [{ id: 1, canonical_name: "Иванов Иван Иванович", entity_type: "person", content_count: 4, positions_count: 1 }],
          detail: { id: 1, entity_id: 1, canonical_name: "Иванов Иван Иванович", entity_type: "person", description: "Министр тестирования", positions: [{ position_title: "Министр тестирования", organization: "Министерство тестирования" }], media: [{ media_kind: "photo", file_path: "processed/documents/entity_media/photos/ivanov.jpg" }], disclosures: [{ disclosure_year: 2024, income_amount: 1234567.89, source_url: "https://example.test/disclosure/1" }], affiliations: [{ role_type: "director", company_name: "Тестовая компания", source_url: "https://example.test/egrul/1" }], restrictions: [{ restriction_type: "internet_block", target_name: "Сайт example.test", right_category: "internet" }], content: [{ title: "Executive profile snapshot", published_at: "2026-04-25" }], claims: [{ claim_text: "Иванов занимает должность" }], cases: [{ title: "Кейс назначения" }], relations: [{ relation_type: "works_at", from_name: "Иванов Иван Иванович", to_name: "Министерство тестирования" }] },
        },
        relations: {
          items: [
            { id: 41, from_name: "Иванов Иван Иванович", to_name: "Министерство тестирования", relation_type: "works_at", relation_label: "Работает в", layer: "structural", layer_label: "структурная", strength: "strong", detected_by: "official_positions", detected_label: "официальные должности" },
            { id: 42, from_name: "Иванов Иван Иванович", to_name: "Петров Пётр Петрович", relation_type: "same_case_cluster", relation_label: "В одном кейсе", layer: "weak_similarity", layer_label: "кластерная", strength: "medium", detected_by: "case_overlap", detected_label: "case overlap" },
            { id: 43, from_name: "Петров Пётр Петрович", to_name: "Роскомнадзор", relation_type: "works_at", relation_label: "Работает в", layer: "evidence", layer_label: "доказательная", strength: "strong", detected_by: "official_positions", detected_label: "официальные должности" },
          ],
          detail: {
            id: 41,
            from_entity_id: 1,
            to_entity_id: 2,
            from_name: "Иванов Иван Иванович",
            to_name: "Министерство тестирования",
            relation_type: "works_at",
            relation_label: "Работает в",
            layer: "evidence",
            layer_label: "доказательная",
            strength: "strong",
            detected_by: "official_positions",
            detected_label: "официальные должности",
            summary: "Иванов Иван Иванович занимает должность в Министерство тестирования.",
            context_title: "Министерство тестирования Российской Федерации",
            evidence_title: "Executive profile snapshot",
            evidence_content_id: 11,
            bridge_paths: [
              {
                hops: 2,
                label: "Иванов Иван Иванович → Законопроект: 901048-8 · О проекте федерального закона о тестировании → Петров Пётр Петрович",
                nodes: [
                  { id: "entity:1", role: "map_entity", label: "Иванов Иван Иванович", title: "Иванов Иван Иванович", jump_screen: "entities", jump_id: 1 },
                  { id: "bill:71", role: "bridge_bill", label: "Законопроект: 901048-8 · О проекте федерального закона о тестировании", title: "901048-8 · О проекте федерального закона о тестировании" },
                  { id: "entity:3", role: "map_entity", label: "Петров Пётр Петрович", title: "Петров Пётр Петрович", jump_screen: "entities", jump_id: 3 },
                ],
              },
              {
                hops: 3,
                label: "Иванов Иван Иванович → Claim: Иванов Иван Иванович занимает должность министра → Дело: Кейс назначения → Петров Пётр Петрович",
                nodes: [
                  { id: "entity:1", role: "map_entity", label: "Иванов Иван Иванович", title: "Иванов Иван Иванович", jump_screen: "entities", jump_id: 1 },
                  { id: "claim:21", role: "bridge_claim", label: "Claim: Иванов Иван Иванович занимает должность министра", title: "Иванов Иван Иванович занимает должность министра", jump_screen: "claims", jump_id: 21 },
                  { id: "case:31", role: "bridge_case", label: "Дело: Кейс назначения", title: "Кейс назначения", jump_screen: "cases", jump_id: 31 },
                  { id: "entity:3", role: "map_entity", label: "Петров Пётр Петрович", title: "Петров Пётр Петрович", jump_screen: "entities", jump_id: 3 },
                ],
              },
            ],
            evidence_graph: {
              kind: "relation",
              nodes: [
                { id: "entity:1", role: "entity_from", label: "person", title: "Иванов Иван Иванович", meta: "Министр тестирования", description: "Персона, для которой зафиксирована должность.", jump_screen: "entities", jump_id: 1 },
                { id: "relation:41", role: "relation", label: "доказательная", title: "Работает в", meta: "strong · официальные должности", description: "Связь построена по официальному профилю должности." },
                { id: "entity:2", role: "entity_to", label: "organization", title: "Министерство тестирования", meta: "Орган государственной власти", description: "Организация, указанная как место работы.", jump_screen: "entities", jump_id: 2 },
                { id: "context:41", role: "context", label: "Контекст", title: "Министерство тестирования Российской Федерации", meta: "", description: "Полный контекст названия из official position source." },
                { id: "evidence:11", role: "evidence", label: "Evidence", title: "Executive profile snapshot", meta: "официальные должности", description: "Источник, из которого подтверждается связь.", jump_screen: "content", jump_id: 11 },
              ],
              edges: [
                { from: "entity:1", to: "relation:41", label: "источник связи", kind: "entity" },
                { from: "relation:41", to: "entity:2", label: "Работает в", kind: "relation" },
                { from: "context:41", to: "relation:41", label: "контекст", kind: "context" },
                { from: "evidence:11", to: "relation:41", label: "доказательство", kind: "evidence" },
              ],
            },
          },
          map_graph: {
            kind: "relation_map",
            stats: { nodes: 9, edges: 8, bridge_nodes: 5 },
            nodes: [
              { id: "entity:1", role: "map_entity", label: "person", title: "Иванов Иван Иванович", meta: "связей 2", description: "Министр тестирования. Узел пересечения должностных и кейсовых связей.", jump_screen: "entities", jump_id: 1 },
              { id: "entity:2", role: "map_entity", label: "organization", title: "Министерство тестирования", meta: "связей 1", description: "Орган государственной власти.", jump_screen: "entities", jump_id: 2 },
              { id: "entity:3", role: "map_entity", label: "person", title: "Петров Пётр Петрович", meta: "связей 2", description: "Заместитель руководителя. Связан через кейс и должность.", jump_screen: "entities", jump_id: 3 },
              { id: "entity:4", role: "map_entity", label: "organization", title: "Роскомнадзор", meta: "связей 1", description: "Ведомство, в котором работает Петров.", jump_screen: "entities", jump_id: 4 },
              { id: "claim:21", role: "bridge_claim", label: "Claim", title: "Иванов Иван Иванович занимает должность министра", meta: "verified", description: "Claim связывает фигурантов через публикацию и кейс.", jump_screen: "claims", jump_id: 21 },
              { id: "case:31", role: "bridge_case", label: "Дело", title: "Кейс назначения", meta: "oversight", description: "Case агрегирует claim и подтверждения.", jump_screen: "cases", jump_id: 31 },
              { id: "bill:71", role: "bridge_bill", label: "Законопроект", title: "901048-8 · О проекте федерального закона о тестировании", meta: "registered", description: "Общий законопроект, в котором фигуранты соавторы." },
              { id: "contract:81", role: "bridge_contract", label: "Контракт", title: "T-2026-81 · Контракт на тестовую поставку", meta: "2026-04-22", description: "Контрактный узел, объединяющий организацию и фигуранта." },
              { id: "content:11", role: "bridge_content", label: "Контент", title: "Executive profile snapshot", meta: "Минфин · 2026-04-25", description: "Публикация, из которой строится часть claim/evidence слоя.", jump_screen: "content", jump_id: 11 },
            ],
            edges: [
              { id: 41, from: "entity:1", to: "entity:2", label: "Работает в", kind: "structural", strength: "strong", detected_label: "официальные должности", summary: "Иванов Иван Иванович занимает должность в Министерстве тестирования." },
              { id: 42, from: "entity:1", to: "entity:3", label: "В одном кейсе", kind: "weak_similarity", strength: "medium", detected_label: "case overlap", summary: "Иванов Иван Иванович и Петров Пётр Петрович фигурируют в одном кейсе и подтверждаются общими материалами." },
              { id: 43, from: "entity:3", to: "entity:4", label: "Работает в", kind: "evidence", strength: "strong", detected_label: "официальные должности", summary: "Петров Пётр Петрович занимает должность в Роскомнадзоре." },
              { from: "entity:1", to: "claim:21", label: "subject", kind: "claim", summary: "Иванов фигурирует в claim." },
              { from: "content:11", to: "claim:21", label: "источник", kind: "origin", summary: "Публикация служит источником claim." },
              { from: "case:31", to: "claim:21", label: "в деле", kind: "case", summary: "Claim входит в дело." },
              { from: "entity:1", to: "bill:71", label: "соавтор", kind: "bill", summary: "Иванов является соавтором законопроекта." },
              { from: "entity:3", to: "contract:81", label: "supplier", kind: "contract", summary: "Петров выступает стороной контракта." },
            ],
          },
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
