(function reactorV2Module(global) {
  "use strict";

  const FLAG_NAME = "reactor_v2";
  const STORAGE_KEY = "cel.feature.reactor_v2";
  const STYLE_ID = "reactor-v2-styles";

  const ROUTES = [
    { group: "monitoring", section: "content", label: "Inbox", icon: "IN" },
    { group: "analytics", section: "events", label: "Events", icon: "EV" },
    { group: "verification", section: "claims", label: "Facts", icon: "FA" },
    { group: "analytics", section: "entities", label: "Entities", icon: "EN" },
    { group: "verification", section: "review_ops", label: "Review", icon: "RV" },
    { group: "analytics", section: "relations", label: "Graph", icon: "GR" },
    { group: "monitoring", section: "ops247", label: "Operations", icon: "OP" },
  ];

  function isFeatureEnabled(options) {
    if (typeof options?.enabled === "boolean") {
      return options.enabled;
    }
    if (typeof global.CEL_FEATURES?.reactorV2 === "boolean") {
      return global.CEL_FEATURES.reactorV2;
    }
    const documentFlag = document.documentElement.dataset.reactorV2;
    if (documentFlag === "true" || documentFlag === "1") {
      return true;
    }
    const queryFlag = new URLSearchParams(global.location.search).get(FLAG_NAME);
    if (queryFlag === "1" || queryFlag === "true") {
      return true;
    }
    try {
      return global.localStorage.getItem(STORAGE_KEY) === "1";
    } catch (_error) {
      return false;
    }
  }

  function ensureStylesheet(options) {
    if (document.getElementById(STYLE_ID)) {
      return;
    }
    const link = document.createElement("link");
    link.id = STYLE_ID;
    link.rel = "stylesheet";
    link.href = options?.stylesheetUrl || "./reactor_v2.css";
    document.head.append(link);
  }

  function makeElement(tagName, className, text) {
    const element = document.createElement(tagName);
    if (className) {
      element.className = className;
    }
    if (text !== undefined) {
      element.textContent = civicText(text);
    }
    return element;
  }

  class RequestCoordinator {
    constructor() {
      this.requests = new Map();
      this.sequence = 0;
    }

    cancel(scope) {
      const active = this.requests.get(scope);
      active?.controller.abort();
      this.requests.delete(scope);
    }

    cancelAll() {
      for (const scope of this.requests.keys()) {
        this.cancel(scope);
      }
    }

    async run(scope, executor) {
      this.cancel(scope);
      const controller = new AbortController();
      const requestId = ++this.sequence;
      this.requests.set(scope, { controller, requestId });
      try {
        const value = await executor({ signal: controller.signal, requestId });
        const active = this.requests.get(scope);
        if (!active || active.requestId !== requestId || controller.signal.aborted) {
          return { accepted: false, stale: true, requestId };
        }
        return { accepted: true, stale: false, requestId, value };
      } catch (error) {
        if (this.requests.get(scope)?.requestId !== requestId || controller.signal.aborted) {
          return { accepted: false, stale: true, requestId };
        }
        throw error;
      } finally {
        const active = this.requests.get(scope);
        if (active?.requestId === requestId) {
          this.requests.delete(scope);
        }
      }
    }
  }

  class RelationCanvasViewport {
    constructor(options) {
      this.options = options || {};
      this.nodes = [];
      this.edges = [];
      this.transform = { x: 0, y: 0, scale: 1 };
      this.drag = null;
      this.frame = 0;
      this.fullscreenHost = null;
      this.returnMarker = null;
      this.lastFocusedElement = null;
      this.root = this.buildRoot();
      if (typeof global.ResizeObserver === "function") {
        this.resizeObserver = new global.ResizeObserver(() => this.scheduleDraw());
        this.resizeObserver.observe(this.canvas);
      } else {
        this.resizeFallback = () => this.scheduleDraw();
        global.addEventListener("resize", this.resizeFallback);
      }
      this.bindInteractions();
    }

    buildRoot() {
      const root = makeElement("section", "reactor-v2-relation-viewport");
      root.setAttribute("aria-label", "Evidence relation map");

      const toolbar = makeElement("div", "reactor-v2-map-toolbar");
      const title = makeElement("strong", "reactor-v2-map-title", "Evidence graph");
      const controls = makeElement("div", "reactor-v2-map-controls");
      const resetButton = makeElement("button", "reactor-v2-map-button", "Reset view");
      resetButton.type = "button";
      resetButton.dataset.mapAction = "reset";
      const fullscreenButton = makeElement("button", "reactor-v2-map-button", "Fullscreen");
      fullscreenButton.type = "button";
      fullscreenButton.dataset.mapAction = "fullscreen";
      controls.append(resetButton, fullscreenButton);
      toolbar.append(title, controls);

      this.canvas = document.createElement("canvas");
      this.canvas.className = "reactor-v2-map-canvas";
      this.canvas.tabIndex = 0;
      this.canvas.setAttribute("role", "img");
      this.canvas.setAttribute("aria-label", "Interactive evidence relation map");
      this.context = this.canvas.getContext("2d");
      this.status = makeElement("div", "reactor-v2-map-status", "No graph loaded");
      this.status.setAttribute("aria-live", "polite");
      root.append(toolbar, this.canvas, this.status);
      return root;
    }

    attach(host) {
      host.append(this.root);
      this.scheduleDraw();
      return this;
    }

    loadGraph(graph) {
      const maxNodes = Number(this.options.maxVisibleNodes || 2000);
      this.nodes = Array.isArray(graph?.nodes) ? graph.nodes.slice(0, maxNodes) : [];
      const visibleIds = new Set(this.nodes.map((node) => String(node.id)));
      this.edges = Array.isArray(graph?.edges)
        ? graph.edges.filter((edge) => visibleIds.has(String(edge.source)) && visibleIds.has(String(edge.target)))
        : [];
      this.status.textContent = `${this.nodes.length} nodes / ${this.edges.length} evidence paths`;
      this.resetView(false);
    }

    bindInteractions() {
      this.root.addEventListener("click", (event) => {
        const action = event.target.closest("[data-map-action]")?.dataset.mapAction;
        if (action === "reset") {
          this.resetView();
        } else if (action === "fullscreen") {
          this.enterFullscreen();
        } else if (action === "close-fullscreen") {
          this.exitFullscreen();
        }
      });

      this.canvas.addEventListener("pointerdown", (event) => {
        this.canvas.setPointerCapture(event.pointerId);
        this.drag = { pointerId: event.pointerId, x: event.clientX, y: event.clientY };
      });
      this.canvas.addEventListener("pointermove", (event) => {
        if (!this.drag || this.drag.pointerId !== event.pointerId) {
          return;
        }
        this.transform.x += event.clientX - this.drag.x;
        this.transform.y += event.clientY - this.drag.y;
        this.drag.x = event.clientX;
        this.drag.y = event.clientY;
        this.scheduleDraw();
      });
      this.canvas.addEventListener("pointerup", (event) => {
        if (this.drag?.pointerId === event.pointerId) {
          this.drag = null;
        }
      });
      this.canvas.addEventListener(
        "wheel",
        (event) => {
          event.preventDefault();
          const factor = event.deltaY < 0 ? 1.12 : 0.89;
          this.transform.scale = Math.min(4, Math.max(0.2, this.transform.scale * factor));
          this.scheduleDraw();
        },
        { passive: false }
      );
    }

    resetView(announce) {
      this.transform = { x: 0, y: 0, scale: 1 };
      if (announce !== false) {
        this.status.textContent = `View reset - ${this.nodes.length} nodes`;
      }
      this.scheduleDraw();
    }

    scheduleDraw() {
      if (this.frame) {
        return;
      }
      this.frame = global.requestAnimationFrame(() => {
        this.frame = 0;
        this.draw();
      });
    }

    draw() {
      const rect = this.canvas.getBoundingClientRect();
      const ratio = Math.max(1, Math.min(2, global.devicePixelRatio || 1));
      const width = Math.max(1, Math.round(rect.width * ratio));
      const height = Math.max(1, Math.round(rect.height * ratio));
      if (this.canvas.width !== width || this.canvas.height !== height) {
        this.canvas.width = width;
        this.canvas.height = height;
      }
      const context = this.context;
      context.setTransform(ratio, 0, 0, ratio, 0, 0);
      context.clearRect(0, 0, rect.width, rect.height);
      context.fillStyle = "#06100d";
      context.fillRect(0, 0, rect.width, rect.height);
      context.save();
      context.translate(rect.width / 2 + this.transform.x, rect.height / 2 + this.transform.y);
      context.scale(this.transform.scale, this.transform.scale);

      const nodeById = new Map(this.nodes.map((node) => [String(node.id), node]));
      context.lineWidth = 0.8 / this.transform.scale;
      context.strokeStyle = "rgba(111, 176, 153, 0.22)";
      context.beginPath();
      for (const edge of this.edges) {
        const source = nodeById.get(String(edge.source));
        const target = nodeById.get(String(edge.target));
        if (!source || !target) {
          continue;
        }
        context.moveTo(Number(source.x || 0), Number(source.y || 0));
        context.lineTo(Number(target.x || 0), Number(target.y || 0));
      }
      context.stroke();

      for (const node of this.nodes) {
        const radius = Math.max(2.5, Math.min(8, Number(node.weight || 4)));
        context.beginPath();
        context.fillStyle = node.color || "#55d6aa";
        context.arc(Number(node.x || 0), Number(node.y || 0), radius, 0, Math.PI * 2);
        context.fill();
        if (this.nodes.length <= 50 && node.label) {
          context.font = "11px Consolas, monospace";
          context.fillStyle = "#e9f5ef";
          context.fillText(String(node.label).slice(0, 40), Number(node.x || 0) + 10, Number(node.y || 0) + 4);
        }
      }
      context.restore();
    }

    enterFullscreen() {
      if (this.fullscreenHost) {
        return;
      }
      this.lastFocusedElement = document.activeElement;
      this.returnMarker = document.createComment("reactor-v2-map-return");
      this.root.before(this.returnMarker);
      const host = makeElement("div", "reactor-v2-map-fullscreen");
      host.setAttribute("role", "dialog");
      host.setAttribute("aria-modal", "true");
      host.setAttribute("aria-label", "Fullscreen evidence relation map");
      const closeButton = makeElement("button", "reactor-v2-map-button", "Close fullscreen");
      closeButton.type = "button";
      closeButton.dataset.mapAction = "close-fullscreen";
      this.root.querySelector(".reactor-v2-map-controls")?.append(closeButton);
      host.append(this.root);
      document.body.append(host);
      this.fullscreenHost = host;
      this.fullscreenKeyHandler = (event) => {
        if (event.key === "Escape") {
          this.exitFullscreen();
          return;
        }
        if (event.key === "Tab") {
          const focusable = [...this.fullscreenHost.querySelectorAll("button, canvas, [tabindex]")].filter(
            (element) => !element.disabled && element.tabIndex >= 0
          );
          const first = focusable[0];
          const last = focusable[focusable.length - 1];
          if (event.shiftKey && document.activeElement === first) {
            event.preventDefault();
            last?.focus();
          } else if (!event.shiftKey && document.activeElement === last) {
            event.preventDefault();
            first?.focus();
          }
        }
      };
      document.addEventListener("keydown", this.fullscreenKeyHandler);
      closeButton.focus();
      this.scheduleDraw();
    }

    exitFullscreen() {
      if (!this.fullscreenHost || !this.returnMarker) {
        return;
      }
      const closeButton = this.root.querySelector('[data-map-action="close-fullscreen"]');
      closeButton?.remove();
      this.returnMarker.before(this.root);
      this.returnMarker.remove();
      this.fullscreenHost.remove();
      document.removeEventListener("keydown", this.fullscreenKeyHandler);
      this.fullscreenHost = null;
      this.returnMarker = null;
      this.lastFocusedElement?.focus?.();
      this.scheduleDraw();
    }

    destroy() {
      this.exitFullscreen();
      this.resizeObserver?.disconnect();
      if (this.resizeFallback) {
        global.removeEventListener("resize", this.resizeFallback);
      }
      if (this.frame) {
        global.cancelAnimationFrame(this.frame);
      }
      this.root.remove();
    }
  }

  class EvidenceWorkbenchV2 {
    constructor(options) {
      this.options = options || {};
      this.requestCoordinator = new RequestCoordinator();
      this.appShell = null;
      this.observer = null;
      this.reviewPending = false;
    }

    init() {
      if (!isFeatureEnabled(this.options)) {
        return false;
      }
      this.appShell = document.getElementById("app-shell");
      if (!this.appShell || this.appShell.dataset.reactorV2Initialized === "true") {
        return false;
      }
      ensureStylesheet(this.options);
      document.documentElement.classList.add("reactor-v2-enabled");
      this.appShell.classList.add("reactor-v2-shell");
      this.appShell.dataset.reactorV2Initialized = "true";
      this.installNavigationRail();
      this.decorateWorkspace();
      this.enhanceCurrentScreen();
      this.observeLegacyRenders();
      document.dispatchEvent(new CustomEvent("reactor-v2:ready", { detail: { instance: this } }));
      return true;
    }

    installNavigationRail() {
      const rail = makeElement("nav", "reactor-v2-nav-rail");
      rail.id = "reactor-v2-nav-rail";
      rail.setAttribute("aria-label", "Evidence Workbench navigation");
      rail.append(makeElement("div", "reactor-v2-rail-mark", "CEL"));
      for (const route of ROUTES) {
        const button = makeElement("button", "reactor-v2-rail-button");
        button.type = "button";
        button.dataset.group = route.group;
        button.dataset.section = route.section;
        button.setAttribute("aria-label", route.label);
        const icon = makeElement("span", "reactor-v2-rail-icon", route.icon);
        icon.setAttribute("aria-hidden", "true");
        button.append(icon, makeElement("span", "reactor-v2-rail-label", route.label));
        button.addEventListener("click", () => this.navigate(route));
        rail.append(button);
      }
      this.appShell.prepend(rail);
      this.navigationRail = rail;
      this.syncActiveRoute();
    }

    decorateWorkspace() {
      const mainPanel = this.appShell.querySelector(".main-panel");
      const screenPanel = this.appShell.querySelector(".screen-panel");
      const inspectorHost = document.getElementById("detail-overlay-host");
      mainPanel?.classList.add("reactor-v2-workspace");
      screenPanel?.classList.add("reactor-v2-workbench");
      if (inspectorHost) {
        inspectorHost.classList.add("reactor-v2-inspector-host");
        inspectorHost.setAttribute("role", "dialog");
        inspectorHost.setAttribute("aria-modal", "false");
        inspectorHost.setAttribute("aria-label", "Evidence inspector");
        inspectorHost.tabIndex = -1;
      }
    }

    navigate(route) {
      document.dispatchEvent(new CustomEvent("reactor-v2:navigate", { detail: route }));
      const groupButton = document.querySelector(`[data-nav-group="${route.group}"]`);
      if (groupButton && this.appShell.dataset.group !== route.group) {
        groupButton.click();
      }
      global.setTimeout(() => {
        document.querySelector(`[data-nav-section="${route.section}"]`)?.click();
      }, 0);
    }

    syncActiveRoute() {
      const section = this.appShell.dataset.section;
      this.navigationRail?.querySelectorAll("[data-section]").forEach((button) => {
        if (button.dataset.section === section) {
          button.setAttribute("aria-current", "page");
        } else {
          button.removeAttribute("aria-current");
        }
      });
    }

    observeLegacyRenders() {
      const screenRoot = document.getElementById("screen-root");
      if (!screenRoot) {
        return;
      }
      let scheduled = false;
      this.observer = new MutationObserver(() => {
        if (scheduled) {
          return;
        }
        scheduled = true;
        global.requestAnimationFrame(() => {
          scheduled = false;
          this.enhanceCurrentScreen();
        });
      });
      this.observer.observe(screenRoot, { childList: true });
    }

    enhanceCurrentScreen() {
      this.syncActiveRoute();
      const section = this.appShell.dataset.section;
      if (section === "review_ops") {
        this.ensureReviewActions();
      }
      if (section === "ops247") {
        this.ensureOperationsCockpit();
      }
    }

    ensureReviewActions() {
      const screenRoot = document.getElementById("screen-root");
      if (!screenRoot || screenRoot.querySelector(".reactor-v2-review-actions")) {
        return;
      }
      const bar = makeElement("div", "reactor-v2-review-actions");
      bar.setAttribute("role", "toolbar");
      bar.setAttribute("aria-label", "Review task actions");
      const actions = [
        ["approve", "Approve", "confirm"],
        ["reject", "Reject", "danger"],
        ["merge", "Merge", "neutral"],
        ["split", "Split", "neutral"],
        ["defer", "Defer", "neutral"],
        ["request_evidence", "Request evidence", "neutral"],
      ];
      for (const [action, label, tone] of actions) {
        const button = makeElement("button", "reactor-v2-action-button", label);
        button.type = "button";
        button.dataset.reviewAction = action;
        button.dataset.tone = tone;
        button.disabled = typeof this.options.onReviewAction !== "function";
        if (button.disabled) button.title = "Unavailable: no review write handler is configured";
        bar.append(button);
      }
      bar.addEventListener("click", (event) => {
        const button = event.target.closest("[data-review-action]");
        if (button) {
          this.runReviewAction(button.dataset.reviewAction, bar);
        }
      });
      screenRoot.append(bar);
    }

    async runReviewAction(action, bar) {
      if (this.reviewPending) {
        return;
      }
      this.reviewPending = true;
      const buttons = [...bar.querySelectorAll("button")];
      buttons.forEach((button) => {
        button.disabled = true;
      });
      const detail = {
        action,
        section: this.appShell.dataset.section,
        selectedRowId: document.querySelector("[data-row-id].selected")?.dataset.rowId || null,
      };
      try {
        if (typeof this.options.onReviewAction === "function") {
          await this.requestCoordinator.run("review-action", ({ signal, requestId }) =>
            this.options.onReviewAction(detail, { signal, requestId })
          );
        } else {
          document.dispatchEvent(new CustomEvent("reactor-v2:review-action", { detail }));
        }
      } finally {
        this.reviewPending = false;
        buttons.forEach((button) => {
          button.disabled = false;
        });
      }
    }

    ensureOperationsCockpit() {
      const screenRoot = document.getElementById("screen-root");
      if (!screenRoot || screenRoot.querySelector(".reactor-v2-operations-cockpit")) {
        return;
      }
      const cockpit = makeElement("section", "reactor-v2-operations-cockpit");
      cockpit.setAttribute("aria-label", "Operations cockpit");
      const heading = makeElement("div", "reactor-v2-cockpit-title");
      const title = makeElement("h3", "", "Operations control plane");
      const refresh = makeElement("button", "reactor-v2-cockpit-button", "Refresh telemetry");
      refresh.type = "button";
      refresh.addEventListener("click", () => {
        document.dispatchEvent(new CustomEvent("reactor-v2:refresh-operations"));
      });
      heading.append(title, refresh);
      cockpit.append(heading);
      const metrics = [
        ["Pipeline", "Unavailable"],
        ["Evidence gate", "Unavailable"],
        ["Agent queues", "Unavailable"],
        ["Projection", "Unavailable"],
      ];
      for (const [label, value] of metrics) {
        const card = makeElement("div", "reactor-v2-cockpit-card");
        card.append(makeElement("span", "", label), makeElement("strong", "", value));
        cockpit.append(card);
      }
      screenRoot.prepend(cockpit);
    }

    createRelationViewport(host, options) {
      return new RelationCanvasViewport(options).attach(host);
    }

    destroy() {
      this.observer?.disconnect();
      this.requestCoordinator.cancelAll();
      this.navigationRail?.remove();
      document.documentElement.classList.remove("reactor-v2-enabled");
      this.appShell?.classList.remove("reactor-v2-shell");
      delete this.appShell?.dataset.reactorV2Initialized;
    }
  }

  // Keep data keys and backend enum values unchanged; translate display chrome.
  const CIVIC_RU_LABELS = {
  "Monitoring": "\u041c\u043e\u043d\u0438\u0442\u043e\u0440\u0438\u043d\u0433",
  "Elections": "\u0412\u044b\u0431\u043e\u0440\u044b",
  "Threads": "\u0420\u0430\u0441\u0441\u043b\u0435\u0434\u043e\u0432\u0430\u043d\u0438\u044f",
  "Evidence": "\u0414\u043e\u043a\u0430\u0437\u0430\u0442\u0435\u043b\u044c\u0441\u0442\u0432\u0430",
  "Claims": "\u0423\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d\u0438\u044f",
  "Graph": "\u0413\u0440\u0430\u0444",
  "Review": "\u041f\u0440\u043e\u0432\u0435\u0440\u043a\u0430",
  "Legacy archive": "\u0421\u0442\u0430\u0440\u044b\u0439 \u0430\u0440\u0445\u0438\u0432",
  "Return to Civic workbench": "\u0412\u0435\u0440\u043d\u0443\u0442\u044c\u0441\u044f \u0432 Civic",
  "REACTOR / READ ONLY": "REACTOR / \u0422\u041e\u041b\u042c\u041a\u041e \u0427\u0422\u0415\u041d\u0418\u0415",
  "Search records": "\u041f\u043e\u0438\u0441\u043a \u0437\u0430\u043f\u0438\u0441\u0435\u0439",
  "Apply filters / Refresh": "\u041f\u0440\u0438\u043c\u0435\u043d\u0438\u0442\u044c / \u041e\u0431\u043d\u043e\u0432\u0438\u0442\u044c",
  "Previous page": "\u041d\u0430\u0437\u0430\u0434",
  "Next page": "\u0414\u0430\u043b\u0435\u0435",
  "Close inspector": "\u0417\u0430\u043a\u0440\u044b\u0442\u044c \u043a\u0430\u0440\u0442\u043e\u0447\u043a\u0443",
  "Source system ID": "ID \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0430",
  "Object kind": "\u0422\u0438\u043f \u043e\u0431\u044a\u0435\u043a\u0442\u0430",
  "Seen from": "\u041d\u0430\u0431\u043b\u044e\u0434\u0435\u043d\u0438\u0435 \u0441",
  "Seen through": "\u041d\u0430\u0431\u043b\u044e\u0434\u0435\u043d\u0438\u0435 \u043f\u043e",
  "Revision ID": "ID \u0440\u0435\u0432\u0438\u0437\u0438\u0438",
  "Claim status": "\u0421\u0442\u0430\u0442\u0443\u0441 \u0443\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d\u0438\u044f",
  "Created from": "\u0421\u043e\u0437\u0434\u0430\u043d\u043e \u0441",
  "Created through": "\u0421\u043e\u0437\u0434\u0430\u043d\u043e \u043f\u043e",
  "Scope ID": "ID \u0443\u0447\u0430\u0441\u0442\u043a\u0430 \u0431\u044e\u043b\u043b\u0435\u0442\u0435\u043d\u044f",
  "Validation state": "\u0421\u043e\u0441\u0442\u043e\u044f\u043d\u0438\u0435 \u043f\u0440\u043e\u0432\u0435\u0440\u043a\u0438",
  "Publication stage (not verification)": "\u0421\u0442\u0430\u0434\u0438\u044f \u043f\u0443\u0431\u043b\u0438\u043a\u0430\u0446\u0438\u0438",
  "Recorded from": "\u0417\u0430\u043f\u0438\u0441\u0430\u043d\u043e \u0441",
  "Recorded through": "\u0417\u0430\u043f\u0438\u0441\u0430\u043d\u043e \u043f\u043e",
  "Incident state": "\u0421\u0442\u0430\u0442\u0443\u0441 \u0438\u043d\u0446\u0438\u0434\u0435\u043d\u0442\u0430",
  "Stance": "\u041f\u043e\u0437\u0438\u0446\u0438\u044f",
  "Campaign ID": "ID \u043a\u0430\u043c\u043f\u0430\u043d\u0438\u0438",
  "Verification state": "\u0421\u043e\u0441\u0442\u043e\u044f\u043d\u0438\u0435 \u0432\u0435\u0440\u0438\u0444\u0438\u043a\u0430\u0446\u0438\u0438",
  "Claim ID": "ID \u0443\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d\u0438\u044f",
  "Workflow state": "\u0421\u0442\u0430\u0434\u0438\u044f \u0440\u0430\u0431\u043e\u0442\u044b",
  "Event ID": "ID \u0441\u043e\u0431\u044b\u0442\u0438\u044f",
  "Modality": "\u041c\u043e\u0434\u0430\u043b\u044c\u043d\u043e\u0441\u0442\u044c",
  "Assertion state": "\u0421\u0442\u0430\u0442\u0443\u0441 \u0441\u0432\u044f\u0437\u0438",
  "Observed from": "\u041d\u0430\u0431\u043b\u044e\u0434\u0435\u043d\u0438\u0435 \u0441",
  "Observed through": "\u041d\u0430\u0431\u043b\u044e\u0434\u0435\u043d\u0438\u0435 \u043f\u043e",
  "Decision": "\u0420\u0435\u0448\u0435\u043d\u0438\u0435",
  "Source objects and their current immutable revisions. Not live collector telemetry.": "\u0421\u0432\u043e\u0434\u043a\u0430 \u0441\u043e\u0445\u0440\u0430\u043d\u0451\u043d\u043d\u044b\u0445 \u0434\u0430\u043d\u043d\u044b\u0445 \u0438 \u0437\u0430\u0434\u0430\u0447. \u0421\u0447\u0451\u0442\u0447\u0438\u043a\u0438 \u043e\u0431\u0449\u0438\u0435, \u043d\u0435 \u0437\u0430\u0432\u0438\u0441\u044f\u0442 \u043e\u0442 \u0444\u0438\u043b\u044c\u0442\u0440\u0430 \u0441\u043f\u0438\u0441\u043a\u0430. \u042d\u0442\u043e \u043d\u0435 \u043f\u0440\u043e\u0432\u0435\u0440\u043a\u0430 \u0434\u043e\u0441\u0442\u0443\u043f\u043d\u043e\u0441\u0442\u0438 \u043a\u043e\u043b\u043b\u0435\u043a\u0442\u043e\u0440\u043e\u0432.",
  "Recorded election evidence only. Protocol validation, human acceptance, incident state and attributed claims are distinct.": "\u0417\u0430\u043f\u0438\u0441\u0430\u043d\u043d\u044b\u0435 \u0434\u0430\u043d\u043d\u044b\u0435 \u0432\u044b\u0431\u043e\u0440\u043e\u0432. \u041f\u0440\u043e\u0432\u0435\u0440\u043a\u0430 \u043f\u0440\u043e\u0442\u043e\u043a\u043e\u043b\u0430, \u043f\u0440\u0438\u043d\u044f\u0442\u0438\u0435 \u0447\u0435\u043b\u043e\u0432\u0435\u043a\u043e\u043c, \u0441\u0442\u0430\u0442\u0443\u0441 \u0438\u043d\u0446\u0438\u0434\u0435\u043d\u0442\u0430 \u0438 \u0437\u0430\u044f\u0432\u043b\u0435\u043d\u0438\u044f \u0440\u0430\u0437\u0434\u0435\u043b\u0435\u043d\u044b.",
  "Persisted investigation questions and workflow states. Unavailable until the Civic schema is installed; no synthetic threads.": "\u0421\u043e\u0445\u0440\u0430\u043d\u0451\u043d\u043d\u044b\u0435 \u0432\u043e\u043f\u0440\u043e\u0441\u044b \u0440\u0430\u0441\u0441\u043b\u0435\u0434\u043e\u0432\u0430\u043d\u0438\u0439 \u0438 \u0441\u0442\u0430\u0434\u0438\u0438 \u0440\u0430\u0431\u043e\u0442\u044b. \u0422\u0440\u0435\u0431\u0443\u0435\u0442\u0441\u044f \u0441\u0445\u0435\u043c\u0430 Civic; \u0432\u044b\u043c\u044b\u0448\u043b\u0435\u043d\u043d\u044b\u0435 \u0437\u0430\u043f\u0438\u0441\u0438 \u043d\u0435 \u043f\u043e\u043a\u0430\u0437\u044b\u0432\u0430\u044e\u0442\u0441\u044f.",
  "Core evidence items and Civic claim evidence links are separate records. Verification and authenticity states are not a guarantee of truth.": "\u0414\u043e\u043a\u0430\u0437\u0430\u0442\u0435\u043b\u044c\u0441\u0442\u0432\u0430 \u0438 \u0441\u0441\u044b\u043b\u043a\u0438 \u043d\u0430 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0438 \u0443\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d\u0438\u0439 \u0445\u0440\u0430\u043d\u044f\u0442\u0441\u044f \u043e\u0442\u0434\u0435\u043b\u044c\u043d\u043e. \u0421\u0442\u0430\u0442\u0443\u0441\u044b \u043f\u0440\u043e\u0432\u0435\u0440\u043a\u0438 \u0438 \u043f\u043e\u0434\u043b\u0438\u043d\u043d\u043e\u0441\u0442\u0438 \u043d\u0435 \u0433\u0430\u0440\u0430\u043d\u0442\u0438\u0440\u0443\u044e\u0442 \u0438\u0441\u0442\u0438\u043d\u043d\u043e\u0441\u0442\u044c.",
  "Civic claims, active derived facts and attributed election claims are separate records. None is automatically verified.": "\u0423\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d\u0438\u044f Civic, \u0432\u044b\u0432\u0435\u0434\u0435\u043d\u043d\u044b\u0435 \u0444\u0430\u043a\u0442\u044b \u0438 \u0437\u0430\u044f\u0432\u043b\u0435\u043d\u0438\u044f \u043e \u0432\u044b\u0431\u043e\u0440\u0430\u0445 \u0440\u0430\u0437\u0434\u0435\u043b\u0435\u043d\u044b. \u041d\u0438 \u043e\u0434\u043d\u0430 \u0437\u0430\u043f\u0438\u0441\u044c \u043d\u0435 \u0441\u0447\u0438\u0442\u0430\u0435\u0442\u0441\u044f \u0430\u0432\u0442\u043e\u043c\u0430\u0442\u0438\u0447\u0435\u0441\u043a\u0438 \u043f\u0440\u043e\u0432\u0435\u0440\u0435\u043d\u043d\u043e\u0439.",
  "Promoted assertions from the active relation generation, for this filtered page only. Not the full graph.": "\u041f\u0440\u0438\u043d\u044f\u0442\u044b\u0435 \u0441\u0432\u044f\u0437\u0438 \u0430\u043a\u0442\u0438\u0432\u043d\u043e\u0439 \u043f\u0440\u043e\u0435\u043a\u0446\u0438\u0438 \u0442\u043e\u043b\u044c\u043a\u043e \u0434\u043b\u044f \u0442\u0435\u043a\u0443\u0449\u0435\u0439 \u0441\u0442\u0440\u0430\u043d\u0438\u0446\u044b. \u042d\u0442\u043e \u043d\u0435 \u043f\u043e\u043b\u043d\u044b\u0439 \u0433\u0440\u0430\u0444.",
  "Knowledge verification review records. Read-only: no approve/reject actions or operations queue integration.": "\u0417\u0430\u043f\u0438\u0441\u0438 \u043f\u0440\u043e\u0432\u0435\u0440\u043a\u0438 \u0437\u043d\u0430\u043d\u0438\u0439. \u0422\u043e\u043b\u044c\u043a\u043e \u0447\u0442\u0435\u043d\u0438\u0435: \u043f\u0440\u0438\u043d\u044f\u0442\u0438\u0435 \u0438 \u043e\u0442\u043a\u043b\u043e\u043d\u0435\u043d\u0438\u0435 \u043d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u043d\u044b.",
  "Knowledge records": "\u0411\u0430\u0437\u0430 \u0437\u043d\u0430\u043d\u0438\u0439",
  "Agent tasks": "\u0417\u0430\u0434\u0430\u0447\u0438 \u0430\u0433\u0435\u043d\u0442\u043e\u0432",
  "Source objects": "\u041e\u0431\u044a\u0435\u043a\u0442\u044b \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u043e\u0432",
  "Source revisions": "\u0420\u0435\u0432\u0438\u0437\u0438\u0438 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u043e\u0432",
  "Civic claims": "\u0423\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d\u0438\u044f Civic",
  "Evidence items": "\u0414\u043e\u043a\u0430\u0437\u0430\u0442\u0435\u043b\u044c\u0441\u0442\u0432\u0430",
  "Evidence links": "\u0421\u0432\u044f\u0437\u0438 \u0441 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0430\u043c\u0438",
  "Investigation threads": "\u0420\u0430\u0441\u0441\u043b\u0435\u0434\u043e\u0432\u0430\u043d\u0438\u044f",
  "Active fenced tasks": "\u0410\u043a\u0442\u0438\u0432\u043d\u044b\u0435 \u0437\u0430\u0434\u0430\u0447\u0438 \u0441 \u0430\u0440\u0435\u043d\u0434\u043e\u0439",
  "Pending / retry": "\u041e\u0436\u0438\u0434\u0430\u044e\u0442 / \u043f\u043e\u0432\u0442\u043e\u0440",
  "Failed tasks": "\u041e\u0448\u0438\u0431\u043a\u0438 \u0437\u0430\u0434\u0430\u0447",
  "Last recorded heartbeat (UTC)": "\u041f\u043e\u0441\u043b\u0435\u0434\u043d\u0438\u0439 heartbeat (UTC)",
  "Unavailable": "\u041d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u043d\u043e",
  "Unknown": "\u041d\u0435\u0438\u0437\u0432\u0435\u0441\u0442\u043d\u043e",
  "Snapshot, not a live health check.": "\u0421\u043d\u0438\u043c\u043e\u043a \u0434\u0430\u043d\u043d\u044b\u0445, \u043d\u0435 \u043f\u0440\u043e\u0432\u0435\u0440\u043a\u0430 \u0440\u0430\u0431\u043e\u0442\u043e\u0441\u043f\u043e\u0441\u043e\u0431\u043d\u043e\u0441\u0442\u0438.",
  "Active = running + owner/token + unexpired lease/deadline. No tokens are exposed.": "\u0410\u043a\u0442\u0438\u0432\u043d\u044b\u0435: running, \u0432\u043b\u0430\u0434\u0435\u043b\u0435\u0446 \u0438 \u0442\u043e\u043a\u0435\u043d, \u0434\u0435\u0439\u0441\u0442\u0432\u0443\u044e\u0449\u0438\u0435 \u0430\u0440\u0435\u043d\u0434\u0430 \u0438 \u0441\u0440\u043e\u043a. \u0422\u043e\u043a\u0435\u043d\u044b \u043d\u0435 \u043f\u0435\u0440\u0435\u0434\u0430\u044e\u0442\u0441\u044f.",
  "Evidence graph": "\u0413\u0440\u0430\u0444 \u0434\u043e\u043a\u0430\u0437\u0430\u0442\u0435\u043b\u044c\u0441\u0442\u0432",
  "Reset view": "\u0421\u0431\u0440\u043e\u0441\u0438\u0442\u044c \u0432\u0438\u0434",
  "Fullscreen": "\u041d\u0430 \u0432\u0435\u0441\u044c \u044d\u043a\u0440\u0430\u043d"
};
  Object.assign(CIVIC_RU_LABELS, {
  "Collection": "Сбор данных",
  "Active profile": "Активный профиль",
  "Start collection": "Начать сбор",
  "Get report": "Получить отчёт",
  "Stop collection": "Остановить сбор",
  "Refresh status": "Обновить статус",
  "Runtime status": "Состояние сборщика",
  "Report status": "Состояние отчёта",
  "Report paths": "Файлы отчёта",
  "Checking runtime...": "Проверка состояния...",
  "No report requested.": "Отчёт ещё не запрошен.",
  "Collection continues after closing this window. Reports use a bounded read-only snapshot; sources and public publication remain read-only.": "Сбор продолжается после закрытия окна. Отчёт использует ограниченный снимок только для чтения; исходные файлы и публичная публикация не изменяются.",
  "Full text": "\u041f\u043e\u043b\u043d\u044b\u0439 \u0442\u0435\u043a\u0441\u0442",
  "Debug JSON": "JSON \u0434\u043b\u044f \u043e\u0442\u043b\u0430\u0434\u043a\u0438",
  "More related records": "\u041e\u0441\u0442\u0430\u043b\u044c\u043d\u044b\u0435 \u0441\u0432\u044f\u0437\u0430\u043d\u043d\u044b\u0435 \u0437\u0430\u043f\u0438\u0441\u0438",
  "No related records.": "\u0421\u0432\u044f\u0437\u0430\u043d\u043d\u044b\u0435 \u0437\u0430\u043f\u0438\u0441\u0438 \u043e\u0442\u0441\u0443\u0442\u0441\u0442\u0432\u0443\u044e\u0442.",
  "status": "\u0421\u0442\u0430\u0442\u0443\u0441",
  "state": "\u0421\u043e\u0441\u0442\u043e\u044f\u043d\u0438\u0435",
  "decision": "\u0420\u0435\u0448\u0435\u043d\u0438\u0435",
  "polarity": "\u041f\u043e\u043b\u044f\u0440\u043d\u043e\u0441\u0442\u044c",
  "modality": "\u041c\u043e\u0434\u0430\u043b\u044c\u043d\u043e\u0441\u0442\u044c",
  "attribution": "\u0410\u0442\u0440\u0438\u0431\u0443\u0446\u0438\u044f",
  "attributed to": "\u041a\u0442\u043e \u0443\u0442\u0432\u0435\u0440\u0436\u0434\u0430\u0435\u0442",
  "claim text": "\u0422\u0435\u043a\u0441\u0442 \u0443\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d\u0438\u044f",
  "source revision id": "\u0420\u0435\u0432\u0438\u0437\u0438\u044f \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0430",
  "verification state": "\u0412\u0435\u0440\u0438\u0444\u0438\u043a\u0430\u0446\u0438\u044f",
  "authenticity state": "\u041f\u043e\u0434\u043b\u0438\u043d\u043d\u043e\u0441\u0442\u044c",
  "stance": "\u041f\u043e\u0437\u0438\u0446\u0438\u044f",
  "origin key": "\u041f\u0440\u043e\u0438\u0441\u0445\u043e\u0436\u0434\u0435\u043d\u0438\u0435",
  "locator json": "\u041b\u043e\u043a\u0430\u0442\u043e\u0440 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0430",
  "claim key": "\u041a\u043b\u044e\u0447 \u0443\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d\u0438\u044f",
  "created at": "\u0421\u043e\u0437\u0434\u0430\u043d\u043e",
  "Record preview": "\u041a\u0430\u0440\u0442\u043e\u0447\u043a\u0430 \u0437\u0430\u043f\u0438\u0441\u0438"
});
  function civicText(text) {
    return document.documentElement?.lang === "ru" ? (CIVIC_RU_LABELS[text] || text) : text;
  }

  const CIVIC_SCREENS = {
    monitoring: { label: "Monitoring", resources: ["monitoring"], note: "Source objects and their current immutable revisions. Not live collector telemetry." },
    elections: { label: "Elections", resources: ["elections", "scopes", "protocols", "incidents", "election_claims"], note: "Recorded election evidence only. Protocol validation, human acceptance, incident state and attributed claims are distinct." },
    threads: { label: "Threads", resources: ["threads"], note: "Persisted investigation questions and workflow states. Unavailable until the Civic schema is installed; no synthetic threads." },
    evidence: { label: "Evidence", resources: ["evidence", "claim_evidence"], note: "Core evidence items and Civic claim evidence links are separate records. Verification and authenticity states are not a guarantee of truth." },
    claims: { label: "Claims", resources: ["claims", "facts", "election_claims"], note: "Civic claims, active derived facts and attributed election claims are separate records. None is automatically verified." },
    graph: { label: "Graph", resources: ["graph"], note: "Promoted assertions from the active relation generation, for this filtered page only. Not the full graph." },
    review: { label: "Review", resources: ["review"], note: "Knowledge verification review records. Read-only: no approve/reject actions or operations queue integration." },
  };

  const CIVIC_FILTERS = {
    monitoring: { source_id: "Source system ID", status: "Object kind", date_from: "Seen from", date_to: "Seen through" },
    protocols: { scope_id: "Scope ID", status: "Validation state", publication_status: "Publication stage (not verification)", date_from: "Recorded from", date_to: "Recorded through" },
    incidents: { scope_id: "Scope ID", status: "Incident state", date_from: "Recorded from", date_to: "Recorded through" },
    election_claims: { scope_id: "Scope ID", status: "Stance", date_from: "Recorded from", date_to: "Recorded through" },
    scopes: { campaign_id: "Campaign ID" },
    evidence: { revision_id: "Revision ID", status: "Verification state", date_from: "Recorded from", date_to: "Recorded through" },
    claim_evidence: { claim_id: "Claim ID", revision_id: "Revision ID", status: "Verification state", date_from: "Created from", date_to: "Created through" },
    claims: { revision_id: "Revision ID", status: "Claim status", date_from: "Created from", date_to: "Created through" },
    threads: { status: "Workflow state", date_from: "Created from", date_to: "Created through" },
    facts: { event_id: "Event ID", status: "Modality", date_from: "Recorded from", date_to: "Recorded through" },
    graph: { event_id: "Event ID", status: "Assertion state", date_from: "Observed from", date_to: "Observed through" },
    review: { status: "Decision", date_from: "Created from", date_to: "Created through" },
  };

  class CivicWorkbench {
    constructor(options) {
      this.options = options;
      this.requests = new RequestCoordinator();
      this.active = true;
      this.screen = "monitoring";
      this.resource = "monitoring";
      this.filters = {};
      this.cursors = [null];
      ensureStylesheet();
      this.legacy = document.getElementById("app-shell");
      this.legacy.hidden = true;
      document.documentElement.classList.add("reactor-v2-enabled");
      this.root = makeElement("div", "app-shell reactor-v2-shell civic-shell");
      this.root.id = "civic-workbench";
      this.nav = makeElement("nav", "reactor-v2-nav-rail");
      this.nav.setAttribute("aria-label", "Civic workbench screens");
      this.nav.append(makeElement("strong", "reactor-v2-rail-mark", "CIVIC"));
      for (const [key, config] of Object.entries(CIVIC_SCREENS)) {
        const button = makeElement("button", "reactor-v2-rail-button", config.label);
        button.type = "button";
        button.dataset.civicScreen = key;
        button.addEventListener("click", () => this.navigate(key));
        this.nav.append(button);
      }
      const archive = makeElement("button", "reactor-v2-rail-button", "Legacy archive");
      archive.type = "button";
      archive.addEventListener("click", () => this.showLegacy());
      this.nav.append(archive);
      this.main = makeElement("main", "main-panel reactor-v2-workspace");
      this.root.append(this.nav, this.main);
      this.legacy.after(this.root);
      this.returnButton = makeElement("button", "secondary-btn civic-return", "Return to Civic workbench");
      this.returnButton.type = "button";
      this.returnButton.hidden = true;
      this.returnButton.addEventListener("click", () => {
        this.active = true;
        this.legacy.hidden = true;
        this.root.hidden = false;
        this.returnButton.hidden = true;
        document.documentElement.classList.add("reactor-v2-enabled");
        this.navigate(this.screen);
      });
      this.legacy.before(this.returnButton);
      this.buildCollectionControls();
      this.navigate("monitoring");
      this.refreshCollectionStatus();
      // One request at a time; never poll hidden archives or background windows.
      this.collectionTimer = global.setInterval(() => {
        if (this.active && !document.hidden) this.refreshCollectionStatus();
      }, 2000);
      global.addEventListener("pagehide", () => global.clearInterval(this.collectionTimer), { once: true });
    }

    buildCollectionControls() {
      this.collectionPanel = makeElement("section", "glass-panel civic-collection");
      this.collectionPanel.setAttribute("aria-label", civicText("Collection"));
      this.collectionPanel.append(makeElement("h2", "", "Collection"));
      const profile = makeElement("p", "civic-profile", `${civicText("Active profile")}: `);
      this.collectionTitle = makeElement("span", "");
      this.collectionProfile = makeElement("code", "", "...");
      profile.append(this.collectionTitle, this.collectionProfile);
      this.collectionPanel.append(profile);
      const actions = makeElement("div", "civic-collection-actions");
      for (const [action, method, label] of [
        ["start", "civicStartCollection", "Start collection"],
        ["report", "civicExportReport", "Get report"],
        ["stop", "civicStopCollection", "Stop collection"],
        ["status", "civicCollectionStatus", "Refresh status"],
      ]) {
        const button = makeElement("button", "secondary-btn", label);
        button.type = "button";
        button.dataset.civicControl = action;
        button.addEventListener("click", () => this.collectionAction(method, action));
        actions.append(button);
      }
      this.collectionRuntime = makeElement("p", "civic-runtime-status", "Checking runtime...");
      this.collectionRuntime.setAttribute("role", "status");
      this.collectionMessage = makeElement("p", "civic-collection-message");
      this.collectionMessage.setAttribute("role", "status");
      this.collectionReport = makeElement("p", "civic-report-status", "No report requested.");
      this.collectionReport.setAttribute("role", "status");
      this.collectionPaths = makeElement("ul", "civic-report-paths");
      this.collectionPaths.setAttribute("aria-label", civicText("Report paths"));
      this.collectionPanel.append(actions, this.collectionRuntime, this.collectionMessage,
        this.collectionReport, this.collectionPaths, makeElement("p", "civic-collection-note",
          "Collection continues after closing this window. Reports use a bounded read-only snapshot; sources and public publication remain read-only."));
    }

    async refreshCollectionStatus() {
      return this.collectionAction("civicCollectionStatus", "status");
    }

    async collectionAction(method, action) {
      if (this.collectionBusy) return;
      this.collectionBusy = true;
      this.updateCollectionButtons();
      try {
        const payload = await this.options.call(method);
        if (!payload || typeof payload !== "object") throw new Error("Collection service unavailable");
        if (payload.profile_path) this.collectionProfile.textContent = String(payload.profile_path);
        if (payload.title || payload.profile_id) this.collectionTitle.textContent = `${String(payload.title || payload.profile_id).slice(0, 300)} / `;
        if (action !== "report") {
          const status = payload.status || (payload.running === true ? "running" : "unknown");
          this.collectionState = status;
          if (action === "status" && status !== "unavailable" && payload.ok !== false && Array.isArray(payload.domain_modules)) {
            const elections = this.nav.querySelector('[data-civic-screen="elections"]');
            elections.hidden = !payload.domain_modules.includes("elections");
            if (elections.hidden && this.screen === "elections") this.navigate("monitoring");
          }
          this.collectionRuntime.textContent = `${civicText("Runtime status")}: ${status}`
            + (payload.pid ? ` / PID ${payload.pid}` : "")
            + (payload.model_state ? ` / models: ${String(payload.model_state).slice(0, 100)}` : "")
            + (Number.isFinite(payload.heartbeat_age_seconds) ? ` / heartbeat: ${payload.heartbeat_age_seconds}s` : "")
            + (payload.error ? ` / ${String(payload.error).slice(0, 1000)}` : "");
        }
        if (action !== "status") {
          this.collectionMessage.textContent = String(payload.error || payload.message || payload.status || "").slice(0, 1000);
        }
        if (payload.report) this.renderCollectionReport(payload.report);
      } catch (error) {
        this.collectionState = "unavailable";
        this.collectionRuntime.textContent = `${civicText("Runtime status")}: unavailable / ${String(error.message).slice(0, 1000)}`;
      } finally {
        this.collectionBusy = false;
        this.updateCollectionButtons();
      }
    }

    renderCollectionReport(report) {
      this.reportPending = ["running", "pending", "report_pending"].includes(report.status);
      if (!this.reportPending && this.collectionMessage.textContent === "report_pending") this.collectionMessage.textContent = "";
      this.collectionReport.textContent = `${civicText("Report status")}: ${report.status || "unknown"}`
        + (report.error ? ` / ${String(report.error).slice(0, 1000)}` : "");
      this.collectionPaths.replaceChildren();
      // Bounded rendering, plain text only: report paths never become executable URLs.
      const paths = report.paths || report.report_paths || {};
      const entries = typeof paths === "string" ? [["report", paths]] : Object.entries(paths);
      for (const [name, path] of entries.slice(0, 20)) {
        this.collectionPaths.append(makeElement("li", "", `${name}: ${String(path).slice(0, 4096)}`));
      }
    }

    updateCollectionButtons() {
      this.collectionPanel.querySelectorAll("[data-civic-control]").forEach((button) => {
        const action = button.dataset.civicControl;
        button.disabled = this.collectionBusy || (action === "report" && this.reportPending)
          || (action === "start" && ["running", "starting", "stop_requested", "stopping"].includes(this.collectionState));
      });
      this.collectionPanel.setAttribute("aria-busy", String(Boolean(this.collectionBusy)));
    }

    showLegacy() {
      this.active = false;
      this.requests.cancelAll();
      this.closeInspector();
      this.viewport?.destroy();
      this.viewport = null;
      this.root.hidden = true;
      this.legacy.hidden = false;
      this.returnButton.hidden = false;
      document.documentElement.classList.remove("reactor-v2-enabled");
      this.options.onLegacy?.();
    }

    navigate(screen, resource) {
      this.requests.cancelAll();
      this.viewport?.destroy();
      this.viewport = null;
      this.screen = screen;
      const config = CIVIC_SCREENS[screen];
      this.resource = resource || config.resources[0];
      this.filters = {};
      this.cursors = [null];
      this.main.replaceChildren();
      this.nav.querySelectorAll("[data-civic-screen]").forEach((button) => {
        button.setAttribute("aria-current", button.dataset.civicScreen === screen ? "page" : "false");
      });
      const heading = makeElement("header", "glass-panel civic-heading");
      heading.append(makeElement("div", "eyebrow", "REACTOR / READ ONLY"), makeElement("h1", "", config.label), makeElement("p", "", config.note));
      if (config.resources.length > 1) {
        const select = makeElement("select", "glass-input");
        select.setAttribute("aria-label", "Record type");
        for (const value of config.resources) {
          const option = makeElement("option", "", value === "facts" ? "Active derived facts" : value.replaceAll("_", " "));
          option.value = value;
          select.append(option);
        }
        select.value = this.resource;
        select.addEventListener("change", () => this.navigate(screen, select.value));
        heading.append(select);
      }
      const form = makeElement("form", "civic-filters");
      for (const [key, label] of Object.entries({ query: "Search records", ...(CIVIC_FILTERS[this.resource] || {}) })) {
        const wrapper = makeElement("label", "", label);
        const input = makeElement("input", "glass-input");
        input.name = key;
        input.type = key.startsWith("date_") ? "date" : "search";
        input.maxLength = key === "query" ? 256 : 128;
        wrapper.append(input);
        form.append(wrapper);
      }
      const apply = makeElement("button", "secondary-btn", "Apply filters / Refresh");
      apply.type = "submit";
      form.append(apply);
      form.addEventListener("submit", (event) => {
        event.preventDefault();
        this.filters = Object.fromEntries(new FormData(form).entries());
        this.cursors = [null];
        this.loadPage();
      });
      heading.append(form);
      this.status = makeElement("p", "civic-status");
      this.status.setAttribute("role", "status");
      this.summary = makeElement("section", "civic-summary");
      this.summary.hidden = screen !== "monitoring";
      this.summary.setAttribute("aria-label", civicText("Monitoring"));
      this.content = makeElement("section", "glass-panel reactor-v2-workbench civic-results");
      this.inspector = makeElement("aside", "reactor-v2-inspector-host civic-inspector");
      this.inspector.hidden = true;
      this.inspector.setAttribute("role", "dialog");
      this.inspector.setAttribute("aria-modal", "false");
      this.inspector.setAttribute("aria-label", "Record inspector");
      this.inspector.addEventListener("keydown", (event) => {
        if (event.key === "Escape") this.closeInspector();
      });
      this.main.append(this.collectionPanel, heading, this.summary, this.status, this.content, this.inspector);
      this.loadPage();
    }

    closeInspector() {
      this.requests.cancel("detail");
      if (this.inspector) {
        this.inspector.hidden = true;
        this.inspector.replaceChildren();
      }
    }

    async call(method, payload) {
      const result = await this.options.call(method, JSON.stringify(payload));
      return result || { availability: "unavailable", reason: "Reactor bridge is unavailable. No demo records are substituted." };
    }

    async loadPage() {
      this.closeInspector();
      this.viewport?.destroy();
      this.viewport = null;
      this.content.replaceChildren();
      this.summary.replaceChildren();
      this.status.textContent = "Loading read-only Reactor records...";
      const resource = this.resource;
      const filters = { ...this.filters, cursor: this.cursors.at(-1), limit: 25 };
      try {
        const reply = await this.requests.run("page", () => this.call("getCivicPage", { resource, filters }));
        if (!reply.accepted || !this.active) return;
        const page = reply.value;
        if (resource === "monitoring") this.renderSummary(page.summary);
        if (page.availability === "unavailable") {
          this.status.textContent = `Unavailable: ${page.reason}`;
          return;
        }
        this.status.textContent = page.items.length
          ? `${page.total} matching records / page ${this.cursors.length}. Text previews capped at ${page.text_limit} characters. Select a record to inspect.`
          : "Empty: no records match these filters in the available Reactor schema.";
        if (resource === "graph" && page.graph?.nodes.length) {
          const graphHost = makeElement("div", "civic-graph");
          this.content.append(graphHost);
          this.viewport = new RelationCanvasViewport({ maxVisibleNodes: 100 }).attach(graphHost);
          const count = page.graph.nodes.length;
          this.viewport.loadGraph({ ...page.graph, nodes: page.graph.nodes.map((node, index) => ({
            ...node, x: 150 * Math.cos(index * Math.PI * 2 / count), y: 150 * Math.sin(index * Math.PI * 2 / count),
          })) });
        }
        const table = makeElement("table", "civic-table");
        const keys = Object.keys(page.items[0] || {});
        const head = makeElement("tr");
        keys.forEach((key) => head.append(makeElement("th", "", key.replaceAll("_", " "))));
        const thead = makeElement("thead");
        thead.append(head);
        table.append(thead);
        const body = makeElement("tbody");
        for (const item of page.items) {
          const tr = makeElement("tr");
          for (const key of keys) {
            const cell = makeElement("td");
            if (key === "id") {
              const inspect = makeElement("button", "secondary-btn", `#${item.id}`);
              inspect.type = "button";
              inspect.setAttribute("aria-label", `Inspect record ${item.id}`);
              inspect.addEventListener("click", () => this.inspect(item.id, inspect));
              cell.append(inspect);
            } else cell.textContent = item[key] == null ? "Not recorded" : String(item[key]);
            tr.append(cell);
          }
          body.append(tr);
        }
        table.append(body);
        const scroller = makeElement("div", "civic-table-scroll");
        scroller.append(table);
        this.content.append(scroller);
        const pager = makeElement("div", "civic-pager");
        const previous = makeElement("button", "secondary-btn", "Previous page");
        previous.disabled = this.cursors.length <= 1;
        previous.addEventListener("click", () => { this.cursors.pop(); this.loadPage(); });
        const next = makeElement("button", "secondary-btn", "Next page");
        next.disabled = !page.next_cursor;
        next.addEventListener("click", () => { this.cursors.push(page.next_cursor); this.loadPage(); });
        pager.append(previous, next);
        this.content.append(pager);
      } catch (_error) {
        this.status.textContent = "Unavailable: read request failed. Refresh to retry.";
      }
    }

    renderSummary(summary) {
      const sections = {
        knowledge: { title: "Knowledge records", labels: { sources: "Source objects", revisions: "Source revisions", claims: "Civic claims", evidence: "Evidence items", evidence_links: "Evidence links", threads: "Investigation threads" } },
        ops: { title: "Agent tasks", labels: { active_fenced: "Active fenced tasks", pending: "Pending / retry", failed: "Failed tasks", last_heartbeat: "Last recorded heartbeat (UTC)" } },
      };
      for (const [key, config] of Object.entries(sections)) {
        const data = summary?.[key];
        const group = makeElement("section", "civic-summary-group");
        group.dataset.summary = key;
        group.dataset.availability = data?.availability || "unavailable";
        group.append(makeElement("h2", "", config.title));
        const cards = makeElement("div", "civic-metric-grid");
        for (const [metric, label] of Object.entries(config.labels)) {
          const card = makeElement("div", "civic-metric");
          card.dataset.metric = metric;
          const value = data?.values?.[metric];
          const missing = value == null ? (data?.availability === "available" ? "Unknown" : "Unavailable") : String(value);
          card.append(makeElement("span", "", label), makeElement("strong", "", missing));
          cards.append(card);
        }
        group.append(cards);
        if (data?.availability === "unavailable" || !data) {
          group.append(makeElement("p", "civic-metric-warning", "Unavailable"));
          group.title = data?.reason || "Summary not supplied by bridge";
        }
        group.append(makeElement("p", "civic-metric-note", key === "ops"
          ? "Active = running + owner/token + unexpired lease/deadline. No tokens are exposed."
          : "Snapshot, not a live health check."));
        this.summary.append(group);
      }
    }

    recordFields(record) {
      const list = makeElement("dl", "civic-record-fields");
      const priority = ["status", "state", "decision", "stance", "verification_state", "authenticity_state",
        "polarity", "modality", "attribution", "attributed_to", "source_revision_id", "origin_key", "locator", "locator_json"];
      const rank = (key, value) => String(value ?? "").length > 300 ? 100 : (priority.includes(key) ? priority.indexOf(key) : 50);
      const fields = Object.entries(record).sort(([a, av], [b, bv]) => rank(a, av) - rank(b, bv));
      for (const [key, value] of fields) {
        const text = value == null ? civicText("Unknown") : (typeof value === "object" ? JSON.stringify(value) : String(value));
        const field = makeElement("div", "civic-record-field");
        field.dataset.field = key;
        field.append(makeElement("dt", "", key.replaceAll("_", " ")));
        const description = makeElement("dd");
        if (text.length > 300) {
          field.classList.add("civic-record-field-long");
          description.append(makeElement("p", "civic-text-preview", text.slice(0, 297) + "..."));
          const expanded = makeElement("details", "civic-long-text");
          expanded.append(makeElement("summary", "", "Full text"));
          expanded.addEventListener("toggle", () => {
            if (expanded.open && !expanded.querySelector("pre")) expanded.append(makeElement("pre", "civic-detail", text));
          });
          description.append(expanded);
        } else description.textContent = text;
        field.append(description);
        list.append(field);
      }
      return list;
    }

    async inspect(id, trigger) {
      this.inspector.hidden = false;
      this.inspector.replaceChildren();
      const close = makeElement("button", "secondary-btn", "Close inspector");
      close.type = "button";
      close.addEventListener("click", () => { this.closeInspector(); trigger.focus(); });
      const body = makeElement("div");
      body.textContent = "Loading record...";
      this.inspector.append(close, body);
      close.focus();
      try {
        const reply = await this.requests.run("detail", () => this.call("getCivicDetail", { resource: this.resource, id }));
        if (!reply.accepted || !this.active) return;
        const payload = reply.value;
        body.replaceChildren();
        if (!payload.detail) {
          body.textContent = payload.reason || "Record is no longer available in this projection.";
          return;
        }
        body.append(makeElement("h2", "", `Record #${id}`), makeElement("p", "", `Read-only preview: fields capped at ${payload.text_limit} characters; related lists at ${payload.related_limit} rows and 1024 characters per field. Text may be truncated. Media and external links are not opened.`));
        body.append(this.recordFields(payload.detail));
        const related = Object.entries(payload.related || {}).sort(([a], [b]) =>
          Number(b === "evidence_links" || b === "evidence") - Number(a === "evidence_links" || a === "evidence"));
        for (const [name, section] of related) {
          body.append(makeElement("h3", "", name.replaceAll("_", " ")));
          const records = makeElement("section", "civic-related-records");
          records.dataset.related = name;
          const addRecords = (host, items) => items.forEach((item) => {
            const card = makeElement("article", "civic-related-card");
            card.append(this.recordFields(item));
            host.append(card);
          });
          addRecords(records, section.items.slice(0, 3));
          if (!section.items.length) records.append(makeElement("p", "", "No related records."));
          if (section.items.length > 3) {
            const more = makeElement("details", "civic-related-more");
            more.append(makeElement("summary", "", "More related records"));
            more.addEventListener("toggle", () => {
              if (more.open && !more.querySelector("article")) addRecords(more, section.items.slice(3));
            });
            records.append(more);
          }
          body.append(records);
          if (section.has_more) body.append(makeElement("p", "", "Additional related rows not loaded (bounded inspector)."));
        }
        const debug = makeElement("details", "civic-debug-json");
        debug.append(makeElement("summary", "", "Debug JSON"));
        debug.addEventListener("toggle", () => {
          if (debug.open && !debug.querySelector("pre")) debug.append(makeElement("pre", "civic-detail", JSON.stringify(payload, null, 2)));
        });
        body.append(debug);
      } catch (_error) {
        body.textContent = "Unavailable: inspector request failed.";
      }
    }
  }

  const api = {
    CivicWorkbench,
    mountCivic(options) { return new CivicWorkbench(options); },
    EvidenceWorkbenchV2,
    RelationCanvasViewport,
    RequestCoordinator,
    isFeatureEnabled,
    init(options) {
      const instance = new EvidenceWorkbenchV2(options);
      return instance.init() ? instance : null;
    },
  };

  global.CELReactorV2 = api;
  // Legacy enhancement remains opt-in for external callers. Actual workbench
  // startup belongs to app.js after the backend setting has been checked.
  api.enhanceLegacy = () => {
    if (isFeatureEnabled()) return api.init();
    return null;
  };
})(window);
