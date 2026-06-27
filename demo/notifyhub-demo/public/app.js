const state = {
  bootstrap: null,
  catalogs: {
    clients: [],
    templates: [],
    providerBindings: [],
    routingPolicies: [],
    preferencePolicies: [],
    callbackRoutes: [],
    webhookSubscriptions: [],
    providerAccounts: [],
  },
  requests: new Map(),
  requestOrder: [],
  webhookEvents: [],
  providerEvents: [],
  providerAccountIDs: new Map(),
  channels: new Map(),
  eventSource: null,
  adminEvents: [],
  activeClient: null,
  ready: false,
};

const els = {
  channelGrid: document.querySelector("#channelGrid"),
  requestFeed: document.querySelector("#requestFeed"),
  webhookFeed: document.querySelector("#webhookFeed"),
  providerEventFeed: document.querySelector("#providerEventFeed"),
  profileGrid: document.querySelector("#profileGrid"),
  adminGrid: document.querySelector("#adminGrid"),
  adminFeed: document.querySelector("#adminFeed"),
  adminSummary: document.querySelector("#adminSummary"),
  clientSpotlight: document.querySelector("[data-client-spotlight]"),
  clientSelect: document.querySelector("[data-client-select]"),
  stats: {
    accepted: document.querySelector('[data-stat="accepted"]'),
    dispatched: document.querySelector('[data-stat="dispatched"]'),
    webhooks: document.querySelector('[data-stat="webhooks"]'),
    failed: document.querySelector('[data-stat="failed"]'),
  },
  livePill: document.querySelector("[data-live-pill]"),
};

const LOCAL_STORAGE_KEY = "notifyhub-demo.request-order";
const CLIENT_CREDENTIALS_KEY = "notifyhub-demo.client-credentials";
const ACTIVE_CLIENT_KEY = "notifyhub-demo.active-client";
const STORAGE_VERSION_KEY = "notifyhub-demo.storage-version";
const STORAGE_VERSION = "3";
const PAGE = document.body?.dataset?.page || "home";

function clearNotifyHubStorage() {
  const keysToRemove = [];
  for (let index = 0; index < localStorage.length; index += 1) {
    const key = localStorage.key(index);
    if (!key) continue;
    if (key.startsWith("notifyhub-demo.")) {
      keysToRemove.push(key);
    }
  }
  for (const key of keysToRemove) {
    localStorage.removeItem(key);
  }
  localStorage.setItem(STORAGE_VERSION_KEY, STORAGE_VERSION);
}

function ensureNotifyHubStorageVersion() {
  try {
    if (localStorage.getItem(STORAGE_VERSION_KEY) !== STORAGE_VERSION) {
      clearNotifyHubStorage();
    }
  } catch {
    // If storage is unavailable, keep the demo working with in-memory state.
  }
}

function isEditingForm() {
  const active = document.activeElement;
  return Boolean(active?.closest?.(".channel-form, .admin-form"));
}

const ADMIN_WORKFLOWS = [
  {
    key: "client",
    title: "1. Create client",
    endpoint: "/v1/clients",
    method: "POST",
    description: "Issue a client API key and restrict which channels this service may send.",
    button: "Create client",
    accent: "#5eead4",
    fields: [
      { key: "client_name", label: "Client name", type: "text", value: "billing-service", placeholder: "billing-service" },
      { key: "allowed_channels", label: "Allowed channels", type: "textarea", rows: 3, value: "email, sms, whatsapp, push, webhook", placeholder: "email, sms, whatsapp, push, webhook" },
      { key: "enabled", label: "Enabled", type: "select", value: "true", options: [
        { value: "true", label: "Enabled" },
        { value: "false", label: "Disabled" },
      ] },
    ],
    buildPayload(values) {
      return {
        tenant_id: values.tenant_id?.trim() || generateDemoTenantID(),
        client_name: values.client_name?.trim(),
        allowed_channels: parseDelimitedList(values.allowed_channels),
        enabled: values.enabled === "true",
      };
    },
  },
  {
    key: "provider-account",
    title: "2. Register provider account",
    endpoint: "/v1/provider-accounts",
    method: "POST",
    description: "Store public provider config separately from secret references.",
    button: "Create provider account",
    accent: "#60a5fa",
    fields: [
      { key: "provider_key", label: "Provider key", type: "select", value: "smtp-email", options: [
        { value: "smtp-email", label: "smtp-email" },
        { value: "gupshup-sms", label: "gupshup-sms" },
        { value: "gupshup-whatsapp", label: "gupshup-whatsapp" },
        { value: "fcm-push", label: "fcm-push" },
      ] },
      { key: "display_name", label: "Display name", type: "text", value: "Primary email account", placeholder: "Primary email account" },
      { key: "channel", label: "Channel", type: "select", value: "email", options: [
        { value: "email", label: "email" },
        { value: "sms", label: "sms" },
        { value: "whatsapp", label: "whatsapp" },
        { value: "push", label: "push" },
      ] },
      { key: "enabled", label: "Enabled", type: "select", value: "true", options: [
        { value: "true", label: "Enabled" },
        { value: "false", label: "Disabled" },
      ] },
      { key: "config", label: "Config JSON", type: "textarea", rows: 8, value: JSON.stringify({
        host: "smtp.gmail.com",
        port: "587",
        user: "no-reply@example.com",
        from_email: "no-reply@example.com",
      }, null, 2), placeholder: "{ ... }", help: "Only non-secret provider settings belong here. Put credentials in Secret refs JSON." },
      { key: "secret_refs", label: "Secret refs JSON", type: "textarea", rows: 8, value: JSON.stringify({
        password: {
          ref: "secret://demo-tenant/providers/smtp-email/password",
          material_type: "secret_string",
        },
      }, null, 2), placeholder: "{ ... }", help: "Paste a JSON map of secret names to file:// or secret:// refs. The connector resolves them at send time." },
    ],
    buildPayload(values) {
      return {
        tenant_id: values.tenant_id?.trim() || generateDemoTenantID(),
        provider_key: values.provider_key?.trim(),
        display_name: values.display_name?.trim(),
        channel: values.channel?.trim(),
        enabled: values.enabled === "true",
        config: parseJSONMap(values.config, "config"),
        secret_refs: parseJSONMap(values.secret_refs, "secret_refs"),
      };
    },
  },
  {
    key: "binding",
    title: "3. Add provider binding",
    endpoint: "/v1/provider-bindings",
    method: "POST",
    description: "Attach a provider account to a binding set and connector endpoint.",
    button: "Create binding",
    accent: "#a78bfa",
    fields: [
      { key: "channel", label: "Channel", type: "select", value: "email", options: [
        { value: "email", label: "email" },
        { value: "sms", label: "sms" },
        { value: "whatsapp", label: "whatsapp" },
        { value: "push", label: "push" },
      ] },
      { key: "binding_set", label: "Binding set", type: "text", value: "tenant-a-email", placeholder: "tenant-a-email" },
      { key: "provider_account_id", label: "Provider account ID", type: "text", value: "", placeholder: "provider-account-id", help: "Usually auto-filled from Step 2 after you create the provider account." },
      { key: "connector_name", label: "Connector name", type: "text", value: "connector-email", placeholder: "connector-email" },
      { key: "endpoint_url", label: "Endpoint URL", type: "text", value: "http://connector-email:8091", placeholder: "http://connector-email:8091" },
      { key: "priority", label: "Priority", type: "text", value: "10", placeholder: "10" },
      { key: "enabled", label: "Enabled", type: "select", value: "true", options: [
        { value: "true", label: "Enabled" },
        { value: "false", label: "Disabled" },
      ] },
    ],
    buildPayload(values) {
      return {
        channel: values.channel?.trim(),
        binding_set: values.binding_set?.trim(),
        provider_account_id: values.provider_account_id?.trim(),
        connector_name: values.connector_name?.trim(),
        endpoint_url: values.endpoint_url?.trim(),
        enabled: values.enabled === "true",
        priority: Number(values.priority || 0),
      };
    },
  },
  {
    key: "template",
    title: "4. Add a template",
    endpoint: "/v1/templates",
    method: "POST",
    description: "Store channel and language variants in the control plane.",
    button: "Create template",
    accent: "#34d399",
    fields: [
      { key: "template_key", label: "Template key", type: "text", value: "billing_email_v1", placeholder: "billing_email_v1" },
      { key: "channel", label: "Channel", type: "select", value: "email", options: [
        { value: "email", label: "email" },
        { value: "sms", label: "sms" },
        { value: "whatsapp", label: "whatsapp" },
        { value: "push", label: "push" },
      ] },
      { key: "language_code", label: "Language code", type: "text", value: "en", placeholder: "en" },
      { key: "subject_template", label: "Subject template", type: "textarea", rows: 3, value: "Payment received for {{recipient_name}}", placeholder: "Payment received for {{recipient_name}}" },
      { key: "body_template", label: "Body template", type: "textarea", rows: 6, value: "Hello {{recipient_name}}, your payment {{reference_id}} is confirmed.", placeholder: "Hello {{recipient_name}}..." },
      { key: "metadata", label: "Metadata JSON", type: "textarea", rows: 6, value: JSON.stringify({ media_type: "text" }, null, 2), placeholder: "{ ... }" },
      { key: "enabled", label: "Enabled", type: "select", value: "true", options: [
        { value: "true", label: "Enabled" },
        { value: "false", label: "Disabled" },
      ] },
    ],
    buildPayload(values) {
      return {
        template_key: values.template_key?.trim(),
        channel: values.channel?.trim(),
        language_code: values.language_code?.trim(),
        subject_template: values.subject_template?.trim(),
        body_template: values.body_template?.trim(),
        metadata: parseJSONMap(values.metadata, "metadata"),
        enabled: values.enabled === "true",
      };
    },
  },
  {
    key: "webhook",
    title: "5. Subscribe downstream webhook",
    endpoint: "/v1/webhook-subscriptions",
    method: "POST",
    description: "Forward normalized lifecycle events to another system.",
    button: "Create subscription",
    accent: "#c084fc",
    optional: true,
    fields: [
      { key: "target_url", label: "Target URL", type: "text", value: "http://host.docker.internal:8788/hooks/notification-events", placeholder: "https://example.com/webhooks" },
      { key: "enabled", label: "Enabled", type: "select", value: "true", options: [
        { value: "true", label: "Enabled" },
        { value: "false", label: "Disabled" },
      ] },
    ],
    buildPayload(values) {
      return {
        target_url: values.target_url?.trim(),
        enabled: values.enabled === "true",
      };
    },
  },
  {
    key: "preference",
    title: "6. Apply user preference",
    endpoint: "/v1/preference-policies",
    method: "POST",
    description: "Suppress channels before dispatch starts.",
    button: "Save preference",
    accent: "#38bdf8",
    optional: true,
    fields: [
      { key: "user_id", label: "User ID", type: "text", value: "notifyhub-demo-user", placeholder: "user-123" },
      { key: "channel", label: "Channel", type: "select", value: "email", options: [
        { value: "email", label: "email" },
        { value: "sms", label: "sms" },
        { value: "whatsapp", label: "whatsapp" },
        { value: "push", label: "push" },
      ] },
      { key: "is_enabled", label: "Is enabled", type: "select", value: "true", options: [
        { value: "true", label: "Enabled" },
        { value: "false", label: "Disabled" },
      ] },
    ],
    buildPayload(values) {
      return {
        user_id: values.user_id?.trim(),
        channel: values.channel?.trim(),
        is_enabled: values.is_enabled === "true",
      };
    },
  },
  {
    key: "callback",
    title: "7. Register callback route",
    endpoint: "/v1/callback-routes",
    method: "POST",
    description: "Configure provider callback verification and inbound reply handling.",
    button: "Create callback route",
    accent: "#fb7185",
    optional: true,
    fields: [
      { key: "provider_key", label: "Provider key", type: "select", value: "gupshup-whatsapp", options: [
        { value: "gupshup-whatsapp", label: "gupshup-whatsapp" },
        { value: "karix-whatsapp", label: "karix-whatsapp" },
        { value: "gupshup-sms", label: "gupshup-sms" },
        { value: "karix-sms", label: "karix-sms" },
      ] },
      { key: "provider_account_id", label: "Provider account ID", type: "text", value: "", placeholder: "provider-account-id", help: "Usually auto-filled from Step 2, but you can override it for another account under the same provider key." },
      { key: "callback_path", label: "Callback path", type: "text", value: "/v1/providers/gupshup-whatsapp/provider-account-id/callbacks", placeholder: "/v1/providers/<provider_key>/<provider_account_id>/callbacks", help: "Use a unique path per provider account and match the provider dashboard exactly." },
      { key: "verification_mode", label: "Verification mode", type: "select", value: "shared_secret", options: [
        { value: "none", label: "none" },
        { value: "shared_secret", label: "shared_secret" },
        { value: "hmac_sha256", label: "hmac_sha256" },
      ] },
      { key: "verification_secret_ref", label: "Verification secret ref JSON", type: "textarea", rows: 5, value: JSON.stringify({
        ref: "secret://demo-tenant/providers/gupshup-whatsapp/callback-secret",
        material_type: "secret_string",
      }, null, 2), placeholder: "{ ... }", help: "Use a mounted secret file or secret-manager reference, depending on your environment." },
      { key: "enabled", label: "Enabled", type: "select", value: "true", options: [
        { value: "true", label: "Enabled" },
        { value: "false", label: "Disabled" },
      ] },
    ],
    buildPayload(values) {
      const verificationMode = values.verification_mode?.trim();
      return {
        provider_key: values.provider_key?.trim(),
        provider_account_id: values.provider_account_id?.trim(),
        callback_path: values.callback_path?.trim(),
        verification_mode: verificationMode,
        verification_secret_ref: verificationMode === "none" ? {} : parseJSONMap(values.verification_secret_ref, "verification_secret_ref"),
        enabled: values.enabled === "true",
      };
    },
  },
  {
    key: "routing",
    title: "8. Map event to channels",
    endpoint: "/v1/routing-policies",
    method: "POST",
    description: "Route one business event to one or more channels and a binding set.",
    button: "Create route",
    accent: "#f59e0b",
    optional: true,
    fields: [
      { key: "event_name", label: "Event name", type: "text", value: "billing.invoice.created", placeholder: "billing.invoice.created" },
      { key: "channels", label: "Channels", type: "textarea", rows: 3, value: "email, sms", placeholder: "email, sms" },
      { key: "binding_set", label: "Binding set", type: "text", value: "tenant-a-primary", placeholder: "tenant-a-primary" },
      { key: "priority", label: "Priority", type: "text", value: "10", placeholder: "10" },
      { key: "enabled", label: "Enabled", type: "select", value: "true", options: [
        { value: "true", label: "Enabled" },
        { value: "false", label: "Disabled" },
      ] },
    ],
    buildPayload(values) {
      return {
        event_name: values.event_name?.trim(),
        channels: parseDelimitedList(values.channels),
        binding_set: values.binding_set?.trim(),
        enabled: values.enabled === "true",
        priority: Number(values.priority || 0),
      };
    },
  },
];

const PROVIDER_ACCOUNT_PRESETS = {
  "smtp-email": {
    channel: "email",
    display_name: "Primary email account",
    config: {
      host: "smtp.gmail.com",
      port: "587",
      from_email: "no-reply@example.com",
    },
    secret_refs: {
      password: {
        ref: "file:///run/notification-secrets/demo_email_smtp_password.txt",
        material_type: "secret_string",
        source: "file",
      },
      user: {
        ref: "file:///run/notification-secrets/demo_email_smtp_user.txt",
        material_type: "secret_string",
        source: "file",
      },
    },
  },
  "gupshup-sms": {
    channel: "sms",
    display_name: "Primary SMS account",
    config: {
      sender_id: "DEMO",
      base_url: "https://enterprise.smsgupshup.com/GatewayAPI/rest",
    },
    secret_refs: {
      password: {
        ref: "file:///run/notification-secrets/demo_gupshup_sms_password.txt",
        material_type: "secret_string",
        source: "file",
      },
      username: {
        ref: "file:///run/notification-secrets/demo_gupshup_sms_username.txt",
        material_type: "secret_string",
        source: "file",
      },
    },
  },
  "gupshup-whatsapp": {
    channel: "whatsapp",
    display_name: "Primary WhatsApp account",
    config: {
      username: "2000193848",
      version: "1.1",
      base_url: "https://media.smsgupshup.com/GatewayAPI/rest",
    },
    secret_refs: {
      password: {
        ref: "file:///run/notification-secrets/demo_gupshup_whatsapp_password.txt",
        material_type: "secret_string",
        source: "file",
      },
    },
  },
  "fcm-push": {
    channel: "push",
    display_name: "Primary push account",
    config: {
      project_id: "demo-project",
    },
    secret_refs: {
      service_account_json: {
        ref: "file:///run/notification-secrets/demo_fcm_service_account.json",
        material_type: "secret_json",
        source: "file",
      },
    },
  },
};

const ADMIN_WORKFLOW_PRESETS = {
  client: {
    client_name: "billing-service",
    allowed_channels: "email, sms, whatsapp, push, webhook",
    enabled: "true",
  },
  "provider-account": {
    provider_key: "smtp-email",
    display_name: "Primary email account",
    channel: "email",
    enabled: "true",
  },
  binding: {
    channel: "email",
    binding_set: "tenant-a-email",
    provider_account_id: "provider-account-id",
    connector_name: "connector-email",
    endpoint_url: "http://connector-email:8091",
    priority: "10",
    enabled: "true",
  },
  routing: {
    event_name: "billing.invoice.created",
    channels: "email, sms",
    binding_set: "tenant-a-primary",
    priority: "10",
    enabled: "true",
  },
  template: {
    template_key: "billing_email_v1",
    channel: "email",
    language_code: "en",
    subject_template: "Payment received for {{recipient_name}}",
    body_template: "Hello {{recipient_name}}, your payment {{reference_id}} is confirmed.",
    metadata: JSON.stringify({ media_type: "text" }, null, 2),
    enabled: "true",
  },
  preference: {
    user_id: "notifyhub-demo-user",
    channel: "email",
    is_enabled: "true",
  },
  callback: {
    provider_key: "gupshup-whatsapp",
    provider_account_id: "provider-account-id",
    callback_path: "/v1/providers/gupshup-whatsapp/provider-account-id/callbacks",
    verification_mode: "shared_secret",
    verification_secret_ref: JSON.stringify({
      ref: "file:///run/notification-secrets/demo_whatsapp_callback_secret.txt",
      material_type: "secret_string",
      source: "file",
    }, null, 2),
    enabled: "true",
  },
  webhook: {
    target_url: "http://host.docker.internal:8788/hooks/notification-events",
    enabled: "true",
  },
};

const CHANNEL_TEMPLATES = {
  email: {
    template_key: "demo_email_verification",
    language_code: "en",
    subject_template: "Please verify your e-mail address",
    body_template: "Please click on the link to verify your e-mail address. {{verifyEmailUrl}}",
    metadata: JSON.stringify({ media_type: "text" }, null, 2),
  },
  sms: {
    template_key: "demo_sms_otp",
    language_code: "en",
    subject_template: "",
    body_template: "{{OTP}} is your verification code {{sms_hash}}",
    metadata: JSON.stringify({ media_type: "text" }, null, 2),
  },
  whatsapp: {
    template_key: "demo_whatsapp_otp",
    language_code: "en",
    subject_template: "",
    body_template: "{{otp}} is your verification code.",
    metadata: JSON.stringify({
      editorType: "normal",
      media_type: "text",
      gupshup_template_name: "demo_whatsapp_otp",
      interactive_attributes: '{"footer":"this code expires in 10 minute."}',
    }, null, 2),
  },
  push: {
    template_key: "demo_push_update",
    language_code: "en",
    subject_template: "NotifyHub demo update",
    body_template: "Your notification was routed through NotifyHub.",
    metadata: JSON.stringify({ editorType: "normal", media_type: "text" }, null, 2),
  },
};

const ADMIN_ONBOARDING_STEPS = [
  { text: "Create the client and issue the API key.", optional: false },
  { text: "Register provider accounts with config and secret references.", optional: false },
  { text: "Create provider bindings for each channel.", optional: false },
  { text: "Add templates for each channel and language.", optional: false },
  { text: "Subscribe downstream systems to lifecycle webhooks.", optional: true },
  { text: "Set user preference policies before dispatch begins.", optional: true },
  { text: "Register callback routes before provider webhooks arrive.", optional: true },
  { text: "Map business events to routes and binding sets.", optional: true },
];

const channelLabel = {
  email: "Email",
  sms: "SMS",
  whatsapp: "WhatsApp",
  push: "Push",
};

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatTime(value) {
  if (!value) return "just now";
  const time = new Date(value);
  if (Number.isNaN(time.getTime())) return String(value);
  return time.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function plural(count, singular, pluralized) {
  return `${count} ${count === 1 ? singular : pluralized}`;
}

async function fetchAPIJSON(path, options = {}) {
  const response = await fetch(`/api${path}`, {
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `Request failed with ${response.status}`);
  }
  const text = await response.text();
  if (!text) return null;
  return JSON.parse(text);
}

async function fetchLocalJSON(path, options = {}) {
  const response = await fetch(path, {
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `Request failed with ${response.status}`);
  }
  const text = await response.text();
  if (!text) return null;
  return JSON.parse(text);
}

async function fetchAdminJSON(path, options = {}) {
  const response = await fetch(`/demo/admin${path}`, {
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `Request failed with ${response.status}`);
  }
  const text = await response.text();
  if (!text) return null;
  return JSON.parse(text);
}

function parseDelimitedList(value) {
  return String(value ?? "")
    .split(/[\n,]/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function parseJSONMap(value, fieldName) {
  const text = String(value ?? "").trim();
  if (!text) {
    return {};
  }
  try {
    const parsed = JSON.parse(text);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error("expected an object");
    }
    return parsed;
  } catch (error) {
    throw new Error(`${fieldName} must be valid JSON: ${error.message}`);
  }
}

function generateDemoTenantID() {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return `tenant-${crypto.randomUUID().slice(0, 8)}`;
  }
  return `tenant-${Math.random().toString(36).slice(2, 10)}`;
}

function loadStoredClientCredentials() {
  try {
    const parsed = JSON.parse(localStorage.getItem(CLIENT_CREDENTIALS_KEY) || "[]");
    if (!Array.isArray(parsed)) {
      return [];
    }
    return parsed.filter((item) => item && typeof item === "object" && (item.client_name || item.client_id) && item.api_key);
  } catch {
    return [];
  }
}

function persistStoredClientCredentials(credentials) {
  localStorage.setItem(CLIENT_CREDENTIALS_KEY, JSON.stringify(credentials));
}

function loadActiveClientContext() {
  try {
    const parsed = JSON.parse(localStorage.getItem(ACTIVE_CLIENT_KEY) || "null");
    if (!parsed || typeof parsed !== "object") {
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

function persistActiveClientContext(context) {
  if (!context) {
    localStorage.removeItem(ACTIVE_CLIENT_KEY);
    return;
  }
  localStorage.setItem(ACTIVE_CLIENT_KEY, JSON.stringify(context));
}

function clientContextKey(context) {
  if (!context) return "bootstrap";
  if (context.source === "bootstrap") return "bootstrap";
  return context.client_id ? `client:${context.client_id}` : `client:${context.client_name || "unknown"}`;
}

function currentClientContext() {
  if (state.activeClient) {
    return state.activeClient;
  }
  return {
    source: "bootstrap",
    client_name: state.bootstrap?.client_name || "notifyhub-demo-studio",
    client_id: "bootstrap",
    tenant_id: state.bootstrap?.tenant_id || "",
    api_key: "",
  };
}

function getActiveClientAPIKey() {
  return String(currentClientContext().api_key || "").trim();
}

function activateClientContext(context) {
  const normalized = context?.source === "bootstrap"
    ? {
      source: "bootstrap",
      client_id: "bootstrap",
      client_name: state.bootstrap?.client_name || "notifyhub-demo-studio",
      tenant_id: context?.tenant_id || state.bootstrap?.tenant_id || "",
      api_key: "",
    }
    : {
        source: "client",
        client_id: context.client_id || "",
        client_name: context.client_name || context.display_name || "selected client",
        tenant_id: context.tenant_id || "",
        api_key: context.api_key || "",
      };

  state.activeClient = normalized;
  persistActiveClientContext(normalized);
  renderClientSwitcher();
  renderBootstrapProfile();
}

function rememberClientCredential(client, apiKey) {
  const key = String(apiKey || "").trim();
  if (!key || !client) return null;
  const credential = {
    client_id: client.client_id || "",
    client_name: client.client_name || "",
    tenant_id: client.tenant_id || "",
    api_key: key,
    source: "client",
  };
  const stored = loadStoredClientCredentials();
  const identifier = credential.client_id || credential.client_name;
  const next = stored.filter((item) => (item.client_id || item.client_name) !== identifier);
  next.unshift(credential);
  persistStoredClientCredentials(next);
  return credential;
}

function renderClientSwitcher() {
  if (!els.clientSelect) return;
  const stored = loadStoredClientCredentials();
  const options = [
    {
      value: "bootstrap",
      label: `Bootstrap: ${state.bootstrap?.client_name || "notifyhub-demo-studio"}`,
      context: {
        source: "bootstrap",
        client_id: "bootstrap",
        client_name: state.bootstrap?.client_name || "notifyhub-demo-studio",
        api_key: "",
      },
    },
    ...stored.map((item) => ({
      value: clientContextKey(item),
      label: item.client_name || item.client_id || "saved client",
      context: item,
    })),
  ];

  const seen = new Set();
  const uniqueOptions = [];
  for (const option of options) {
    if (seen.has(option.value)) continue;
    seen.add(option.value);
    uniqueOptions.push(option);
  }

  const activeKey = clientContextKey(currentClientContext());
  els.clientSelect.innerHTML = uniqueOptions
    .map((option) => `<option value="${escapeHtml(option.value)}">${escapeHtml(option.label)}</option>`)
    .join("");
  if (uniqueOptions.some((option) => option.value === activeKey)) {
    els.clientSelect.value = activeKey;
  } else {
    els.clientSelect.value = "bootstrap";
  }
}

function setClientContextByKey(key) {
  const stored = loadStoredClientCredentials();
  if (key === "bootstrap") {
    activateClientContext({ source: "bootstrap" });
    return;
  }
  const match = stored.find((item) => clientContextKey(item) === key);
  if (match) {
    activateClientContext(match);
    return;
  }
  renderClientSwitcher();
}

function summarizeAdminResponse(response) {
  if (!response || typeof response !== "object") {
    return "record";
  }
  const candidates = [
    response.client?.client_name,
    response.display_name,
    response.template_key,
    response.event_name,
    response.subscription_id,
    response.route_id,
    response.policy_id,
    response.provider_account_id,
    response.target_url,
    response.client_id,
  ];
  return candidates.find((value) => typeof value === "string" && value.trim()) || "record";
}

function refreshProviderAccountIndex() {
  const entries = [...(state.catalogs.providerAccounts || [])]
    .filter((item) => item?.provider_account_id && item.enabled !== false)
    .sort((a, b) => new Date(b.updated_at || b.created_at || 0) - new Date(a.updated_at || a.created_at || 0));

  const index = new Map();
  for (const account of entries) {
    const providerKey = String(account.provider_key || "").trim();
    const channel = String(account.channel || "").trim();
    if (providerKey && !index.has(providerKey)) {
      index.set(providerKey, account.provider_account_id);
    }
    if (channel && !index.has(channel)) {
      index.set(channel, account.provider_account_id);
    }
  }
  state.providerAccountIDs = index;
}

function rememberProviderAccount(response) {
  const providerAccountID = String(response?.provider_account_id || "").trim();
  if (!providerAccountID) return;
  const providerKey = String(response?.provider_key || "").trim();
  const channel = String(response?.channel || "").trim();
  if (providerKey) {
    state.providerAccountIDs.set(providerKey, providerAccountID);
  }
  if (channel) {
    state.providerAccountIDs.set(channel, providerAccountID);
  }
}

function providerAccountIDForKey(providerKey, channel = "") {
  const candidates = [providerKey, channel].map((value) => String(value || "").trim()).filter(Boolean);
  for (const candidate of candidates) {
    const providerAccountID = state.providerAccountIDs.get(candidate);
    if (providerAccountID) {
      return providerAccountID;
    }
  }
  return "";
}

function fallbackBootstrap() {
  return {
    app_name: "NotifyHub",
    tagline: "Notification control plane demo.",
    client_name: "notifyhub-demo-studio",
    api_base_url: "/api",
    webhook_inbox: "/demo/webhooks",
    refresh_ms: 3000,
    ready: false,
    bootstrapping: true,
    last_error: "control plane bootstrap pending",
    support_notes: [
      "All live reads go through the local proxy so browser CORS never gets in the way.",
      "The demo seeds its own routing, templates, bindings, and webhook inbox.",
      "Webhook deliveries are captured locally and reflected back in the status rail.",
    ],
    defaults: {
      user_id: "notifyhub-demo-user",
      email: "demo@example.com",
      phone: "+911234567890",
      push_token: "demo-fcm-token",
      recipient: "NotifyHub demo recipient",
      otp: "112233",
      reference: "NH-2026-0619",
      title: "NotifyHub live demo",
      body: "This is a local end-to-end message from NotifyHub.",
      subject: "NotifyHub demo update",
      message: "Your message has been routed through the control plane successfully.",
    },
    channels: [
      {
        key: "email",
        label: "Email",
        accent: "#5eead4",
        icon: "✉",
        route_name: "demo_email_verify",
        binding_set: "tenant-a-email",
        connector_url: "http://connector-email:8091",
        template_key: "demo_email_verification",
        language_code: "en",
        recipient_label: "Email recipient",
        recipient_type: "email",
        default_recipient: "demo@example.com",
        fields: [
          { key: "email", label: "Email", type: "email", placeholder: "demo@example.com", value: "demo@example.com" },
          { key: "reference_id", label: "Reference ID", type: "text", placeholder: "NH-2026-0619", value: "NH-2026-0619" },
        ],
      },
      {
        key: "sms",
        label: "SMS",
        accent: "#f59e0b",
        icon: "⌁",
        route_name: "demo_sms_otp",
        binding_set: "tenant-a-sms",
        connector_url: "http://connector-sms:8092",
        template_key: "demo_sms_otp",
        language_code: "en",
        recipient_label: "Phone number",
        recipient_type: "phone",
        default_recipient: "+911234567890",
        fields: [
          { key: "phone", label: "Phone", type: "tel", placeholder: "+911234567890", value: "+911234567890" },
          { key: "reference_id", label: "Reference ID", type: "text", placeholder: "NH-2026-0619", value: "NH-2026-0619" },
        ],
      },
      {
        key: "whatsapp",
        label: "WhatsApp",
        accent: "#60a5fa",
        icon: "◎",
        route_name: "demo_whatsapp_otp",
        binding_set: "tenant-a-whatsapp",
        connector_url: "http://connector-whatsapp:8095",
        template_key: "demo_whatsapp_otp",
        language_code: "en",
        recipient_label: "Phone number",
        recipient_type: "phone",
        default_recipient: "+911234567890",
        fields: [
          { key: "phone", label: "Phone", type: "tel", placeholder: "+911234567890", value: "+911234567890" },
          { key: "reference_id", label: "Reference ID", type: "text", placeholder: "NH-2026-0619", value: "NH-2026-0619" },
        ],
      },
      {
        key: "push",
        label: "Push",
        accent: "#a78bfa",
        icon: "↗",
        route_name: "demo_push_announcement",
        binding_set: "tenant-a-push",
        connector_url: "http://connector-push:8094",
        template_key: "demo_push_update",
        language_code: "en",
        recipient_label: "FCM token",
        recipient_type: "push_token",
        default_recipient: "demo-fcm-token",
        fields: [
          { key: "push_token", label: "FCM token", type: "textarea", placeholder: "demo-fcm-token", value: "demo-fcm-token", rows: 3 },
          { key: "reference_id", label: "Reference ID", type: "text", placeholder: "NH-2026-0619", value: "NH-2026-0619" },
        ],
      },
    ],
  };
}

async function loadBootstrap() {
  try {
    state.bootstrap = await fetchLocalJSON("/demo/bootstrap");
    state.channels.clear();
    for (const channel of state.bootstrap.channels || []) {
      state.channels.set(channel.key, channel);
    }
  } catch (error) {
    console.warn("bootstrap load failed, using fallback", error);
    state.bootstrap = fallbackBootstrap();
    state.channels.clear();
    for (const channel of state.bootstrap.channels) {
      state.channels.set(channel.key, channel);
    }
  }
  const suffix = PAGE === "admin" ? "Admin API" : "Demo Studio";
  document.title = `${state.bootstrap.app_name || "NotifyHub"} ${suffix}`;
  const storedActiveClient = loadActiveClientContext();
  if (storedActiveClient?.api_key) {
    state.activeClient = storedActiveClient;
  } else if (!state.activeClient) {
    state.activeClient = {
      source: "bootstrap",
      client_id: "bootstrap",
      client_name: state.bootstrap.client_name || "notifyhub-demo-studio",
      api_key: "",
    };
  }
}

async function loadCatalogs() {
  const load = async (path, key) => {
    try {
      return await fetchAPIJSON(path);
    } catch (error) {
      console.warn(`catalog load failed for ${path}`, error);
      return { [key]: [] };
    }
  };

  const [clients, templates, providerBindings, routingPolicies, preferencePolicies, callbackRoutes, webhookSubscriptions, providerAccounts] = await Promise.all([
    load("/v1/clients", "clients"),
    load("/v1/templates", "templates"),
    load("/v1/provider-bindings", "provider_bindings"),
    load("/v1/routing-policies", "routing_policies"),
    load("/v1/preference-policies", "preference_policies"),
    load("/v1/callback-routes", "callback_routes"),
    load("/v1/webhook-subscriptions", "webhook_subscriptions"),
    load("/v1/provider-accounts", "provider_accounts"),
  ]);

  state.catalogs.clients = clients?.clients || [];
  state.catalogs.templates = templates?.templates || [];
  state.catalogs.providerBindings = providerBindings?.provider_bindings || [];
  state.catalogs.routingPolicies = routingPolicies?.routing_policies || [];
  state.catalogs.preferencePolicies = preferencePolicies?.preference_policies || [];
  state.catalogs.callbackRoutes = callbackRoutes?.callback_routes || [];
  state.catalogs.webhookSubscriptions = webhookSubscriptions?.webhook_subscriptions || [];
  state.catalogs.providerAccounts = providerAccounts?.provider_accounts || [];
  refreshProviderAccountIndex();
  renderClientSwitcher();
}

async function loadWebhookEvents() {
  const payload = await fetchLocalJSON("/demo/webhooks");
  state.webhookEvents = payload?.events || [];
}

async function loadProviderEvents() {
  const items = [];
  for (const channel of state.bootstrap.channels) {
    try {
      const payload = await fetchAPIJSON(`/v1/channel-events?channel=${encodeURIComponent(channel.key)}&limit=6`);
      for (const event of payload?.channel_events || []) {
        items.push(event);
      }
    } catch (error) {
      items.push({
        provider_key: channel.key,
        channel: channel.key,
        status: "error",
        event_type: "load_error",
        body: String(error),
        received_at: new Date().toISOString(),
      });
    }
  }
  state.providerEvents = items.sort((a, b) => new Date(b.received_at || 0) - new Date(a.received_at || 0)).slice(0, 24);
}

function storedRequestOrder() {
  try {
    return JSON.parse(localStorage.getItem(LOCAL_STORAGE_KEY) || "[]");
  } catch {
    return [];
  }
}

function persistRequestOrder() {
  localStorage.setItem(LOCAL_STORAGE_KEY, JSON.stringify(state.requestOrder));
}

function seedRequestOrderFromStorage() {
  state.requestOrder = storedRequestOrder().filter(Boolean);
}

function renderBootstrapProfile() {
  if (!els.profileGrid) return;
  const activeClient = currentClientContext();
  const webhooks = state.catalogs.webhookSubscriptions.length;
  const bindings = state.catalogs.providerBindings.length;
  const routes = state.catalogs.routingPolicies.length;
  const templates = state.catalogs.templates.length;
  const accountSummary = state.catalogs.providerAccounts.reduce((acc, item) => {
    acc[item.provider_key] = (acc[item.provider_key] || 0) + 1;
    return acc;
  }, {});

  const rows = [
    ["Active client", activeClient.client_name || "notifyhub-demo-studio"],
    ["Bootstrap client", state.bootstrap.client_name || "notifyhub-demo-studio"],
    ["Webhook inbox", state.bootstrap.webhook_inbox],
    ["Bindings", plural(bindings, "binding", "bindings")],
    ["Routes", plural(routes, "route", "routes")],
    ["Templates", plural(templates, "template", "templates")],
    ["Subscriptions", plural(webhooks, "subscription", "subscriptions")],
    ["Refresh cadence", `${state.bootstrap.refresh_ms / 1000}s auto polling`],
  ];

  els.profileGrid.innerHTML = `
    ${rows
      .map(
        ([label, value]) => `
          <div>
            <strong>${escapeHtml(label)}</strong>
            <span>${escapeHtml(value)}</span>
          </div>
        `
      )
      .join("")}
    <div>
      <strong>Provider mix</strong>
      <span>${escapeHtml(
        Object.entries(accountSummary)
          .map(([key, count]) => `${key}: ${count}`)
          .join(" • ")
      )}</span>
    </div>
  `;

  if (els.clientSpotlight) {
    els.clientSpotlight.textContent = activeClient.client_name || "notifyhub-demo-studio";
  }
}

function renderAdminSummary() {
  if (!els.adminSummary) return;

  const rows = [
    ["Clients", plural(state.catalogs.clients.length, "client", "clients")],
    ["Provider accounts", plural(state.catalogs.providerAccounts.length, "account", "accounts")],
    ["Bindings", plural(state.catalogs.providerBindings.length, "binding", "bindings")],
    ["Routes", plural(state.catalogs.routingPolicies.length, "route", "routes")],
    ["Preferences", plural(state.catalogs.preferencePolicies.length, "policy", "policies")],
    ["Callbacks", plural(state.catalogs.callbackRoutes.length, "route", "routes")],
    ["Templates", plural(state.catalogs.templates.length, "template", "templates")],
    ["Subscriptions", plural(state.catalogs.webhookSubscriptions.length, "subscription", "subscriptions")],
  ];

  const providerNames = state.catalogs.providerAccounts.reduce((acc, item) => {
    acc[item.provider_key] = (acc[item.provider_key] || 0) + 1;
    return acc;
  }, {});

  els.adminSummary.innerHTML = `
    ${rows
      .map(
        ([label, value]) => `
          <div>
            <strong>${escapeHtml(label)}</strong>
            <span>${escapeHtml(value)}</span>
          </div>
        `
      )
      .join("")}
    <div>
      <strong>Provider mix</strong>
      <span>${escapeHtml(
        Object.entries(providerNames)
          .map(([key, count]) => `${key}: ${count}`)
          .join(" • ") || "No provider accounts yet"
      )}</span>
    </div>
  `;
}

function renderAdminFeed() {
  if (!els.adminFeed) return;

  const statusTone = state.bootstrap?.ready ? "ok" : "warn";
  const actionLog = state.adminEvents
    .slice(0, 6)
    .map(
      (event) => `
        <article class="feed-item">
          <div class="feed-item-top">
            <div>
              <strong>${escapeHtml(event.title)}</strong>
              <div class="feed-meta">
                <span>${escapeHtml(event.endpoint)}</span>
                <span>•</span>
                <span>${escapeHtml(event.method)}</span>
              </div>
            </div>
            <span class="status status-${channelStatusTone(event.outcome)}">${escapeHtml(event.outcome)}</span>
          </div>
          <div class="feed-body">${escapeHtml(event.message)}</div>
          <div class="feed-meta">
            <span>${escapeHtml(formatTime(event.created_at))}</span>
          </div>
        </article>
      `
    )
    .join("");

  const checklist = ADMIN_ONBOARDING_STEPS.map(
    (step, index) => `
      <article class="feed-item">
        <div class="feed-item-top">
          <div>
            <strong>Step ${index + 1}</strong>
          </div>
          <div class="workflow-chips">
            ${step.optional ? `<span class="status status-optional">Optional</span>` : ""}
            <span class="status status-${statusTone}">${state.bootstrap?.ready ? "Live" : "Bootstrapping"}</span>
          </div>
        </div>
        <div class="feed-body">${escapeHtml(step.text)}</div>
      </article>
    `
  ).join("");

  els.adminFeed.innerHTML = `
    <div class="feed-group">
      <div class="feed-group-label">Onboarding checklist</div>
      ${checklist}
    </div>
    <div class="feed-group">
      <div class="feed-group-label">Recent admin actions</div>
      ${actionLog || `<div class="placeholder">Use the forms below to provision the control plane.</div>`}
    </div>
  `;
}

function renderAdminConsole() {
  if (!els.adminGrid) return;

  els.adminGrid.innerHTML = ADMIN_WORKFLOWS.map((workflow) => {
    return `
      <article class="admin-card" data-admin-card="${escapeHtml(workflow.key)}" style="--admin-accent:${workflow.accent};">
        <div class="channel-card-top">
          <div class="channel-badge">
            <span class="channel-icon admin-icon" aria-hidden="true">${escapeHtml(workflow.title.split(".")[0] || "◎")}</span>
            <div>
              <strong class="channel-label">${escapeHtml(workflow.title)}</strong>
              <small class="channel-route">${escapeHtml(workflow.endpoint)}</small>
            </div>
          </div>
          <div class="workflow-chips">
            ${workflow.optional ? `<span class="status status-optional">Optional</span>` : ""}
            <span class="channel-status" data-admin-status>Draft</span>
          </div>
        </div>

        <p class="admin-description">${escapeHtml(workflow.description)}</p>
        ${adminWorkflowNote(workflow) ? `<div class="admin-callout">${escapeHtml(adminWorkflowNote(workflow))}</div>` : ""}
        ${workflow.key === "provider-account" ? `<div class="admin-callout" data-provider-account-summary>Latest provider_account_id will appear here after save.</div>` : ""}
        ${workflow.key === "binding" ? `<div class="admin-callout" data-admin-link-summary>Create the provider account in Step 2 and this field will auto-fill.</div>` : ""}
        ${workflow.key === "callback" ? `<div class="admin-callout" data-admin-link-summary>Create the provider account in Step 2 and this field will auto-fill.</div>` : ""}
        ${workflow.key === "template" ? `<div class="admin-callout">If this template is media-based, include a public media URL in the metadata JSON.</div>` : ""}

        <form class="admin-form" data-admin-form="${escapeHtml(workflow.key)}">
          ${workflow.fields
            .map((field) => {
              const fieldId = `admin-${workflow.key}-${field.key}`;
              if (field.type === "select") {
                return `
                  <label class="field">
                    <span>${escapeHtml(field.label)}</span>
                    <select id="${escapeHtml(fieldId)}" data-field-key="${escapeHtml(field.key)}">
                      ${field.options
                        .map((option) => `<option value="${escapeHtml(option.value)}"${option.value === field.value ? " selected" : ""}>${escapeHtml(option.label)}</option>`)
                        .join("")}
                    </select>
                    ${field.help ? `<small class="field-help">${escapeHtml(field.help)}</small>` : ""}
                  </label>
                `;
              }

              if (field.type === "textarea") {
                return `
                  <label class="field">
                    <span>${escapeHtml(field.label)}</span>
                    <textarea id="${escapeHtml(fieldId)}" data-field-key="${escapeHtml(field.key)}" rows="${field.rows || 4}" placeholder="${escapeHtml(field.placeholder || "")}">${escapeHtml(field.value || "")}</textarea>
                    ${field.help ? `<small class="field-help">${escapeHtml(field.help)}</small>` : ""}
                  </label>
                `;
              }

              return `
                <label class="field">
                  <span>${escapeHtml(field.label)}</span>
                  <input id="${escapeHtml(fieldId)}" data-field-key="${escapeHtml(field.key)}" type="${escapeHtml(field.type || "text")}" value="${escapeHtml(field.value || "")}" placeholder="${escapeHtml(field.placeholder || "")}" />
                  ${field.help ? `<small class="field-help">${escapeHtml(field.help)}</small>` : ""}
                </label>
              `;
            })
            .join("")}

          <div class="admin-actions">
            <button class="ghost-button" type="button" data-admin-action="reset">Reset</button>
            <button class="primary-button" type="button" data-admin-action="submit">${escapeHtml(workflow.button)}</button>
          </div>
        </form>

        <pre class="json-preview" data-admin-preview>${escapeHtml(JSON.stringify(workflow.buildPayload(workflow.fields.reduce((acc, field) => {
          acc[field.key] = field.value || "";
          return acc;
        }, {})), null, 2))}</pre>
      </article>
    `;
  }).join("");

  els.adminGrid.querySelectorAll("[data-admin-card]").forEach((card) => {
    if (card.dataset.adminCard === "provider-account") {
      const configInput = card.querySelector('[data-field-key="config"]');
      const secretRefsInput = card.querySelector('[data-field-key="secret_refs"]');
      card.dataset.defaultConfig = configInput?.value || "{}";
      card.dataset.defaultSecretRefs = secretRefsInput?.value || "{}";
    }

    const reset = () => {
      const workflow = ADMIN_WORKFLOWS.find((item) => item.key === card.dataset.adminCard);
      if (!workflow) return;
      card.querySelectorAll("[data-field-key]").forEach((input) => {
        const field = workflow.fields.find((item) => item.key === input.dataset.fieldKey);
        if (!field) return;
        input.value = field.value || "";
      });
      if (workflow.key === "provider-account") {
        syncProviderAccountFields(card);
        updateProviderAccountSummary(card, card.dataset.lastSavedProviderAccountID || "");
      } else {
        syncAdminLinkedFields(card);
      }
      updateAdminPreview(card);
    };

    card.querySelectorAll("[data-field-key]").forEach((input) => {
      input.addEventListener("input", () => {
        syncAdminLinkedFields(card, input.dataset.fieldKey);
        updateAdminPreview(card);
      });
      input.addEventListener("change", () => {
        syncAdminLinkedFields(card, input.dataset.fieldKey);
        updateAdminPreview(card);
      });
    });

    if (card.dataset.adminCard === "provider-account") {
      syncProviderAccountFields(card);
      updateProviderAccountSummary(card, card.dataset.lastSavedProviderAccountID || "");
    } else {
      syncAdminLinkedFields(card);
    }

    card.querySelector('[data-admin-action="reset"]').addEventListener("click", reset);
    card.querySelector('[data-admin-action="submit"]').addEventListener("click", async () => {
      try {
        await submitAdminWorkflow(card);
      } catch (error) {
        console.error(error);
        setAdminStatus(card, "bad", "Failed");
        alert(error.message);
      }
    });
  });

  renderAdminFeed();
  renderAdminSummary();
}

function setAdminStatus(card, tone, text) {
  const status = card.querySelector("[data-admin-status]");
  if (!status) return;
  status.className = `channel-status status status-${tone}`;
  status.textContent = text;
}

function adminCardValues(card) {
  const values = {};
  card.querySelectorAll("[data-field-key]").forEach((input) => {
    values[input.dataset.fieldKey] = input.value;
  });
  const workflowKey = card.dataset.adminCard;
  if (workflowKey === "client" || workflowKey === "provider-account") {
    values.tenant_id = adminWorkflowTenantID(card, workflowKey);
  }
  return values;
}

function adminWorkflowNote(workflow) {
  switch (workflow.key) {
    case "provider-account":
      return "Create this first. The generated provider account ID is reused by later steps automatically.";
    case "binding":
      return "Pick the channel first, then the provider account, connector, and endpoint auto-fill.";
    case "callback":
      return "Pick the provider key first, then the provider account and account-specific callback path auto-fill.";
    case "template":
      return "Use one template per channel and language. Media templates should include media metadata.";
    case "routing":
      return "Optional when the client sends explicit channels in the request body.";
    case "preference":
      return "Optional. Use it to suppress a channel for one user before dispatch starts.";
    case "webhook":
      return "Optional. Subscribe a downstream system to normalized lifecycle updates.";
    default:
      return "";
  }
}

function updateProviderAccountSummary(card, providerAccountID) {
  const summary = card.querySelector("[data-provider-account-summary]");
  if (!summary) return;
  const text = String(providerAccountID || "").trim();
  summary.textContent = text ? `Latest provider_account_id: ${text}` : "Latest provider_account_id will appear here after save.";
}

function updateBindingSummary(card) {
  const summary = card.querySelector("[data-admin-link-summary]");
  if (!summary) return;
  const channel = card.querySelector('[data-field-key="channel"]')?.value?.trim() || "email";
  const providerAccountID = providerAccountIDForKey(defaultProviderKeyForChannel(channel), channel);
  summary.textContent = providerAccountID
    ? `Auto-filled from Step 2: ${providerAccountID}`
    : "Create the provider account in Step 2 and this field will auto-fill.";
}

function updateCallbackSummary(card) {
  const summary = card.querySelector("[data-admin-link-summary]");
  if (!summary) return;
  const providerKey = card.querySelector('[data-field-key="provider_key"]')?.value?.trim() || "gupshup-whatsapp";
  const channel = providerKey.includes("sms") ? "sms" : "whatsapp";
  const providerAccountID = card.querySelector('[data-field-key="provider_account_id"]')?.value?.trim() || providerAccountIDForKey(providerKey, channel);
  summary.textContent = providerAccountID
    ? `Auto-filled from Step 2: ${providerAccountID} and ${callbackPathFor(providerKey, providerAccountID)}`
    : "Create the provider account in Step 2 and this field will auto-fill.";
}

function defaultProviderKeyForChannel(channel) {
  switch (String(channel || "").trim()) {
    case "sms":
      return "gupshup-sms";
    case "whatsapp":
      return "gupshup-whatsapp";
    case "push":
      return "fcm-push";
    case "email":
    default:
      return "smtp-email";
  }
}

function callbackPathFor(providerKey, providerAccountID) {
  const accountSegment = String(providerAccountID || "").trim() || "provider-account-id";
  return `/v1/providers/${providerKey}/${accountSegment}/callbacks`;
}

function adminWorkflowTenantID(card, workflowKey) {
  if (!card) {
    return generateDemoTenantID();
  }

  const existing = String(card.dataset.generatedTenantId || "").trim();
  if (existing) {
    return existing;
  }

  let tenantID = "";
  if (workflowKey === "provider-account") {
    tenantID = String(currentClientContext().tenant_id || "").trim();
  }
  if (!tenantID) {
    tenantID = generateDemoTenantID();
  }
  card.dataset.generatedTenantId = tenantID;
  return tenantID;
}

function workflowPresetValues(workflow, card) {
  if (!workflow) return {};

  if (workflow.key === "provider-account") {
    const channel = card?.querySelector('[data-field-key="channel"]')?.value?.trim() || "email";
    const providerKey = card?.querySelector('[data-field-key="provider_key"]')?.value?.trim() || defaultProviderKeyForChannel(channel);
    const preset = PROVIDER_ACCOUNT_PRESETS[providerKey] || PROVIDER_ACCOUNT_PRESETS[channel];
    if (!preset) return {};
    return {
      provider_key: providerKey,
      display_name: preset.display_name,
      channel: preset.channel,
      enabled: "true",
      config: JSON.stringify(preset.config, null, 2),
      secret_refs: JSON.stringify(preset.secret_refs, null, 2),
    };
  }

  if (workflow.key === "binding") {
    const channel = card?.querySelector('[data-field-key="channel"]')?.value?.trim() || "email";
    const providerAccountID = providerAccountIDForKey(defaultProviderKeyForChannel(channel), channel);
    const preset = ADMIN_WORKFLOW_PRESETS.binding;
    return {
      channel,
      binding_set: channel === "email" ? "tenant-a-email" : channel === "sms" ? "tenant-a-sms" : channel === "whatsapp" ? "tenant-a-whatsapp" : "tenant-a-push",
      provider_account_id: providerAccountID || "",
      connector_name: channel === "email" ? "connector-email" : channel === "sms" ? "connector-sms" : channel === "whatsapp" ? "connector-whatsapp" : "connector-push",
      endpoint_url: channel === "email" ? "http://connector-email:8091" : channel === "sms" ? "http://connector-sms:8092" : channel === "whatsapp" ? "http://connector-whatsapp:8095" : "http://connector-push:8094",
      priority: preset.priority,
      enabled: "true",
    };
  }

  if (workflow.key === "routing") {
    const channel = card?.querySelector('[data-field-key="channels"]')?.value?.trim() || "email, sms";
    return {
      event_name: "billing.invoice.created",
      channels: channel || ADMIN_WORKFLOW_PRESETS.routing.channels,
      binding_set: "tenant-a-primary",
      priority: ADMIN_WORKFLOW_PRESETS.routing.priority,
      enabled: "true",
    };
  }

  if (workflow.key === "template") {
    const channel = card?.querySelector('[data-field-key="channel"]')?.value?.trim() || "email";
    const preset = CHANNEL_TEMPLATES[channel] || CHANNEL_TEMPLATES.email;
    return {
      template_key: preset.template_key,
      channel,
      language_code: preset.language_code,
      subject_template: preset.subject_template,
      body_template: preset.body_template,
      metadata: preset.metadata,
      enabled: "true",
    };
  }

  if (workflow.key === "callback") {
    const providerKey = card?.querySelector('[data-field-key="provider_key"]')?.value?.trim() || "gupshup-whatsapp";
    const channel = providerKey.includes("sms") ? "sms" : "whatsapp";
    const secretPath = channel === "sms"
      ? "file:///run/notification-secrets/demo_gupshup_sms_callback_secret.txt"
      : "file:///run/notification-secrets/demo_whatsapp_callback_secret.txt";
    const providerAccountID = providerAccountIDForKey(providerKey, channel);
    return {
      provider_key: providerKey,
      provider_account_id: providerAccountID,
      callback_path: callbackPathFor(providerKey, providerAccountID),
      verification_mode: providerKey.startsWith("gupshup") ? "shared_secret" : "hmac_sha256",
      verification_secret_ref: JSON.stringify({
        ref: secretPath,
        material_type: "secret_string",
        source: "file",
      }, null, 2),
      enabled: "true",
    };
  }

  return ADMIN_WORKFLOW_PRESETS[workflow.key] || {};
}

function providerAccountPreset(values) {
  return PROVIDER_ACCOUNT_PRESETS[values.provider_key?.trim()] || PROVIDER_ACCOUNT_PRESETS[values.channel?.trim()] || null;
}

function updateAdminProviderReferences() {
  if (!els.adminGrid) return;

  for (const card of els.adminGrid.querySelectorAll("[data-admin-card]")) {
    syncAdminLinkedFields(card);
    updateAdminPreview(card);
  }
}

function syncAdminLinkedFields(card, sourceFieldKey = "") {
  const workflowKey = card.dataset.adminCard;

  if (workflowKey === "provider-account") {
    syncProviderAccountFields(card, sourceFieldKey);
    return;
  }

  if (workflowKey === "binding") {
    const channel = card.querySelector('[data-field-key="channel"]')?.value?.trim() || "email";
    const providerAccountInput = card.querySelector('[data-field-key="provider_account_id"]');
    const bindingSetInput = card.querySelector('[data-field-key="binding_set"]');
    const connectorNameInput = card.querySelector('[data-field-key="connector_name"]');
    const endpointURLInput = card.querySelector('[data-field-key="endpoint_url"]');
    const resolvedProviderAccountID = providerAccountIDForKey(defaultProviderKeyForChannel(channel), channel);

    if (bindingSetInput && (sourceFieldKey === "channel" || !bindingSetInput.value || bindingSetInput.value === "provider-account-id")) {
      bindingSetInput.value = channel === "email" ? "tenant-a-email" : channel === "sms" ? "tenant-a-sms" : channel === "whatsapp" ? "tenant-a-whatsapp" : "tenant-a-push";
    }
    if (connectorNameInput && (sourceFieldKey === "channel" || !connectorNameInput.value || connectorNameInput.value === "connector-email" || connectorNameInput.value === "connector-sms" || connectorNameInput.value === "connector-whatsapp" || connectorNameInput.value === "connector-push")) {
      connectorNameInput.value = channel === "email" ? "connector-email" : channel === "sms" ? "connector-sms" : channel === "whatsapp" ? "connector-whatsapp" : "connector-push";
    }
    if (endpointURLInput && (sourceFieldKey === "channel" || !endpointURLInput.value || endpointURLInput.value === "http://connector-email:8091" || endpointURLInput.value === "http://connector-sms:8092" || endpointURLInput.value === "http://connector-whatsapp:8095" || endpointURLInput.value === "http://connector-push:8094")) {
      endpointURLInput.value = channel === "email" ? "http://connector-email:8091" : channel === "sms" ? "http://connector-sms:8092" : channel === "whatsapp" ? "http://connector-whatsapp:8095" : "http://connector-push:8094";
    }
    if (providerAccountInput && (sourceFieldKey === "channel" || !providerAccountInput.value || providerAccountInput.value === "provider-account-id" || providerAccountInput.value === card.dataset.defaultProviderAccountID)) {
      providerAccountInput.value = resolvedProviderAccountID;
    }
    card.dataset.defaultProviderAccountID = resolvedProviderAccountID || card.dataset.defaultProviderAccountID || "";
    updateBindingSummary(card);
    return;
  }

  if (workflowKey === "callback") {
    const providerKey = card.querySelector('[data-field-key="provider_key"]')?.value?.trim() || "gupshup-whatsapp";
    const providerAccountInput = card.querySelector('[data-field-key="provider_account_id"]');
    const callbackPathInput = card.querySelector('[data-field-key="callback_path"]');
    const channel = providerKey.includes("sms") ? "sms" : "whatsapp";
    const resolvedProviderAccountID = providerAccountIDForKey(providerKey, channel);
    const activeProviderAccountID = providerAccountInput?.value?.trim() || resolvedProviderAccountID;

    if (callbackPathInput && (sourceFieldKey === "provider_key" || sourceFieldKey === "provider_account_id" || !callbackPathInput.value || callbackPathInput.value === card.dataset.defaultCallbackPath)) {
      callbackPathInput.value = callbackPathFor(providerKey, activeProviderAccountID);
    }
    if (providerAccountInput && (sourceFieldKey === "provider_key" || !providerAccountInput.value || providerAccountInput.value === "provider-account-id" || providerAccountInput.value === card.dataset.defaultProviderAccountID)) {
      providerAccountInput.value = resolvedProviderAccountID;
    }
    card.dataset.defaultProviderAccountID = activeProviderAccountID || card.dataset.defaultProviderAccountID || "";
    card.dataset.defaultCallbackPath = callbackPathInput?.value || card.dataset.defaultCallbackPath || "";
    updateCallbackSummary(card);
  }
}

function syncProviderAccountFields(card, sourceFieldKey = "") {
  const values = adminCardValues(card);
  const channel = values.channel?.trim() || "email";
  const providerKey = values.provider_key?.trim() || defaultProviderKeyForChannel(channel);
  const normalizedProviderKey = sourceFieldKey === "channel" ? defaultProviderKeyForChannel(channel) : providerKey;
  const preset = PROVIDER_ACCOUNT_PRESETS[normalizedProviderKey] || PROVIDER_ACCOUNT_PRESETS[channel];
  if (!preset) return;

  const channelInput = card.querySelector('[data-field-key="channel"]');
  const displayNameInput = card.querySelector('[data-field-key="display_name"]');
  const providerKeyInput = card.querySelector('[data-field-key="provider_key"]');
  const configInput = card.querySelector('[data-field-key="config"]');
  const secretRefsInput = card.querySelector('[data-field-key="secret_refs"]');

  if (providerKeyInput && providerKeyInput.value !== normalizedProviderKey) {
    providerKeyInput.value = normalizedProviderKey;
  }
  if (channelInput && channelInput.value !== preset.channel) {
    channelInput.value = preset.channel;
  }
  if (displayNameInput && (sourceFieldKey === "channel" || sourceFieldKey === "provider_key" || !displayNameInput.value || displayNameInput.value === "Primary email account" || displayNameInput.value === "Primary SMS account" || displayNameInput.value === "Primary WhatsApp account" || displayNameInput.value === "Primary push account")) {
    displayNameInput.value = preset.display_name;
  }
  if (configInput && (sourceFieldKey === "channel" || sourceFieldKey === "provider_key" || !configInput.value || configInput.value === "{}" || configInput.value === card.dataset.defaultConfig)) {
    configInput.value = JSON.stringify(preset.config, null, 2);
  }
  if (secretRefsInput && (sourceFieldKey === "channel" || sourceFieldKey === "provider_key" || !secretRefsInput.value || secretRefsInput.value === "{}" || secretRefsInput.value === card.dataset.defaultSecretRefs)) {
    secretRefsInput.value = JSON.stringify(preset.secret_refs, null, 2);
  }
}

function updateAdminPreview(card) {
  const key = card.dataset.adminCard;
  const workflow = ADMIN_WORKFLOWS.find((item) => item.key === key);
  if (!workflow) return;
  const preview = card.querySelector("[data-admin-preview]");
  const values = adminCardValues(card);
  try {
    const payload = workflow.buildPayload(values);
    preview.textContent = JSON.stringify(payload, null, 2);
    setAdminStatus(card, "warn", "Draft");
  } catch (error) {
    preview.textContent = `// ${error.message}`;
    setAdminStatus(card, "bad", "Invalid");
  }
}

async function submitAdminWorkflow(card) {
  const key = card.dataset.adminCard;
  const workflow = ADMIN_WORKFLOWS.find((item) => item.key === key);
  if (!workflow) {
    throw new Error("unknown admin workflow");
  }
  const preview = card.querySelector("[data-admin-preview]");
  const values = adminCardValues(card);
  const payload = workflow.buildPayload(values);

  setAdminStatus(card, "warn", "Saving");
  preview.textContent = JSON.stringify(payload, null, 2);

  const response = await fetchAdminJSON(workflow.endpoint, {
    method: workflow.method,
    body: JSON.stringify(payload),
  });
  if (workflow.key === "provider-account") {
    rememberProviderAccount(response);
    card.dataset.lastSavedProviderAccountID = response?.provider_account_id || "";
    updateProviderAccountSummary(card, response?.provider_account_id || "");
  } else if (workflow.key === "client") {
    const credential = rememberClientCredential(response?.client, response?.api_key);
    if (credential) {
      activateClientContext(credential);
    }
  }

  state.adminEvents = [
    {
      title: workflow.title,
      endpoint: workflow.endpoint,
      method: workflow.method,
      outcome: "accepted",
      message: `Saved ${summarizeAdminResponse(response)}.`,
      created_at: new Date().toISOString(),
    },
    ...state.adminEvents,
  ];
  setAdminStatus(card, "ok", "Saved");
  await loadCatalogs();
  if (workflow.key === "provider-account") {
    updateAdminProviderReferences();
  }
  renderBootstrapProfile();
  renderAdminSummary();
  renderAdminFeed();
  updateStats();
  return response;
}

function updateReadinessControls() {
  const ready = Boolean(state.bootstrap?.ready);
  document.querySelectorAll('[data-action="send-suite"], [data-action="send-channel"]').forEach((button) => {
    button.disabled = false;
    button.setAttribute("aria-disabled", String(!ready));
    button.title = ready ? "" : "The demo is still bootstrapping, but you can still try a send.";
  });

  document.querySelectorAll(".channel-card").forEach((card) => {
    card.classList.toggle("is-disabled", !ready);
  });

  if (els.livePill) {
    if (ready) {
      els.livePill.innerHTML = `<span class="pulse-dot"></span>Ready`;
    } else {
      const message = state.bootstrap?.last_error || "bootstrapping demo resources";
      els.livePill.textContent = `Bootstrapping: ${message}`;
    }
  }
}

function loadStoredFields(channelKey) {
  try {
    return JSON.parse(localStorage.getItem(`notifyhub-demo.form.${channelKey}`) || "{}");
  } catch {
    return {};
  }
}

function saveStoredFields(channelKey, values) {
  localStorage.setItem(`notifyhub-demo.form.${channelKey}`, JSON.stringify(values));
}

function currentChannelValues(channelKey, form) {
  const values = {};
  form.querySelectorAll("[data-field-key]").forEach((input) => {
    values[input.dataset.fieldKey] = input.value;
  });
  return values;
}

function channelBaseVariableKeys(channel) {
  return new Set(["email", "phone", "push_token"]);
}

function normalizeTemplateVariableKey(name) {
  return String(name ?? "").trim().toLowerCase();
}

function templateChoiceValue(template) {
  return `${template.template_key}|||${template.language_code || ""}`;
}

function parseTemplateChoiceValue(value) {
  const [templateKey = "", languageCode = ""] = String(value || "").split("|||");
  return { templateKey, languageCode };
}

function templateLabel(template) {
  const language = template.language_code || "en";
  return `${template.template_key} · ${language}`;
}

function normalizeLanguageCode(code) {
  const normalized = String(code ?? "")
    .trim()
    .toLowerCase()
    .replaceAll("_", "-");
  return normalized || "en";
}

function channelTemplates(channelKey) {
  return (state.catalogs.templates || [])
    .filter((template) => template.channel === channelKey && template.enabled !== false)
    .sort((a, b) => templateLabel(a).localeCompare(templateLabel(b)));
}

function extractTemplateVariables(template) {
  if (!template) return [];
  const seen = new Set();
  const sources = new Set();

  const collect = (value) => {
    if (typeof value === "string") {
      const trimmed = value.trim();
      if (trimmed) {
        sources.add(trimmed);
      }
      return;
    }
    if (Array.isArray(value)) {
      value.forEach(collect);
      return;
    }
    if (value && typeof value === "object") {
      Object.values(value).forEach(collect);
    }
  };

  collect(template.subject_template);
  collect(template.body_template);
  collect(template.title_template);
  collect(template.message_template);
  collect(template.content_template);
  collect(template.text_template);
  collect(template.header_template);
  collect(template.footer_template);
  collect(template.metadata);

  for (const source of sources) {
    for (const match of source.matchAll(/\{\{\s*\.?([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}/g)) {
      const name = String(match[1] || "").trim();
      if (name) {
        seen.add(name);
      }
    }
  }
  return [...seen];
}

function selectedChannelTemplate(channel, formValues) {
  const templates = channelTemplates(channel.key);
  const candidateKey = formValues.template_key?.trim() || channel.template_key;
  const candidateLanguage = normalizeLanguageCode(formValues.language_code?.trim() || channel.language_code);
  return (
    templates.find((template) => template.template_key === candidateKey && normalizeLanguageCode(template.language_code) === candidateLanguage) ||
    templates.find((template) => template.template_key === candidateKey) ||
    templates.find((template) => template.template_key === channel.template_key) ||
    null
  );
}

function templateMediaSpec(template) {
  const metadata = template?.metadata || {};
  const mediaType = normalizeTemplateVariableKey(metadata.media_type || metadata.media_content_type || metadata.content_type);
  return {
    type: mediaType,
    url: String(metadata.media_url || metadata.media_link || "").trim(),
    fileName: String(metadata.media_file_name || metadata.media_name || metadata.filename || "").trim(),
    title: String(metadata.media_title || metadata.title || "").trim(),
  };
}

function templateMetadataEntries(template) {
  const metadata = template?.metadata;
  if (!metadata || typeof metadata !== "object" || Array.isArray(metadata)) {
    return {};
  }

  const entries = {};
  for (const [key, value] of Object.entries(metadata)) {
    if (value === undefined || value === null) {
      continue;
    }
    entries[key] = typeof value === "string" ? value : JSON.stringify(value);
  }
  return entries;
}

function defaultTemplateVariables(channel, template) {
  const variables = {};
  const defaults = channel.fields.reduce((acc, field) => {
    acc[field.key] = field.value || field.placeholder || "";
    return acc;
  }, {});
  const placeholderNames = new Set(extractTemplateVariables(template).map((name) => normalizeTemplateVariableKey(name)));

  if (placeholderNames.has("recipient_name") || defaults.recipient_name || state.bootstrap?.defaults?.recipient) {
    variables.recipient_name = defaults.recipient_name || state.bootstrap.defaults.recipient;
  }
  if (placeholderNames.has("reference_id") || defaults.reference_id || state.bootstrap?.defaults?.reference) {
    variables.reference_id = defaults.reference_id || state.bootstrap.defaults.reference;
  }

  if ((placeholderNames.has("verifyemailurl") || placeholderNames.has("verify_email_url")) && channel.key === "email") {
    variables.verifyemailurl = defaults.verify_email_url || "https://example.com/verify";
  }

  if ((placeholderNames.has("otp") || channel.key === "sms" || channel.key === "whatsapp") && (defaults.otp || state.bootstrap?.defaults?.otp)) {
    variables.otp = defaults.otp || state.bootstrap.defaults.otp;
  }

  if ((placeholderNames.has("sms_hash") || channel.key === "sms") && (defaults.sms_hash || "oCuDdxyOvb+")) {
    variables.sms_hash = defaults.sms_hash || "oCuDdxyOvb+";
  }

  if ((placeholderNames.has("title") || channel.key === "push") && (defaults.title || state.bootstrap?.defaults?.title)) {
    variables.title = defaults.title || state.bootstrap.defaults.title;
  }

  if ((placeholderNames.has("body") || channel.key === "push") && (defaults.body || state.bootstrap?.defaults?.body)) {
    variables.body = defaults.body || state.bootstrap.defaults.body;
  }

  return variables;
}

function buildPayload(channel, formValues) {
  const recipientName = formValues.recipient_name?.trim() || state.bootstrap.defaults.recipient;
  const referenceId = formValues.reference_id?.trim() || state.bootstrap.defaults.reference;
  const recipient = {
    user_id: state.bootstrap.defaults.user_id,
  };

  if (channel.key === "email") {
    recipient.email = formValues.email?.trim() || state.bootstrap.defaults.email;
  } else if (channel.key === "push") {
    recipient.push_token = formValues.push_token?.trim() || state.bootstrap.defaults.push_token;
  } else {
    recipient.phone = formValues.phone?.trim() || state.bootstrap.defaults.phone;
  }

  const template = selectedChannelTemplate(channel, formValues);
  const templateVariables = defaultTemplateVariables(channel, template);
  const media = templateMediaSpec(template);
  const templateMetadata = templateMetadataEntries(template);
  const variables = {
    recipient_name: recipientName,
    reference_id: referenceId,
    ...templateVariables,
  };

  const selectedTemplateVariables = extractTemplateVariables(template).map((name) => normalizeTemplateVariableKey(name));
  const extraTemplateVariables = parseJSONMap(formValues.template_variables, "template_variables");
  Object.assign(variables, extraTemplateVariables);
  for (const key of selectedTemplateVariables) {
    const value = formValues[key];
    if (value !== undefined && String(value).trim() !== "") {
      variables[key] = value;
    }
  }

  return {
    idempotency_key: `demo-${channel.key}-${Date.now()}`,
    event_name: channel.route_name,
    template_key: formValues.template_key?.trim() || channel.template_key,
    language_code: formValues.language_code?.trim() || channel.language_code,
    channels: [channel.key],
    binding_set: formValues.binding_set?.trim() || channel.binding_set,
    recipient,
    variables,
    metadata: {
      source: "notifyhub-demo",
      surface: "studio",
      channel: channel.key,
      ...templateMetadata,
      ...(media.type && media.type !== "text" ? { media_type: media.type } : {}),
      ...(formValues.media_url?.trim() ? { media_url: formValues.media_url.trim() } : {}),
      ...(formValues.media_file_name?.trim() ? { media_file_name: formValues.media_file_name.trim() } : {}),
      ...(formValues.media_title?.trim() ? { media_title: formValues.media_title.trim() } : {}),
    },
    priority: "high",
  };
}

function previewPayload(channel, form) {
  const values = currentChannelValues(channel.key, form);
  const payload = buildPayload(channel, values);
  const preview = form.querySelector(".json-preview");
  preview.textContent = JSON.stringify(payload, null, 2);
}

function setStatus(card, tone, text) {
  const status = card.querySelector("[data-status]");
  status.className = `channel-status status status-${tone}`;
  status.textContent = text;
}

function channelStatusTone(status) {
  switch (status) {
    case "accepted":
    case "dispatched":
    case "delivered":
    case "succeeded":
      return "ok";
    case "failed":
    case "expired":
    case "suppressed":
    case "unsupported":
      return "bad";
    default:
      return "warn";
  }
}

function buildFieldNode(field) {
  const labelNode = document.createElement("label");
  labelNode.className = "field";

  const titleNode = document.createElement("span");
  titleNode.textContent = field.label;
  labelNode.appendChild(titleNode);

  let inputNode;
  if (field.type === "select") {
    inputNode = document.createElement("select");
    for (const option of field.options || []) {
      const optionNode = document.createElement("option");
      optionNode.value = option.value;
      optionNode.textContent = option.label;
      if (option.value === field.value) {
        optionNode.selected = true;
      }
      inputNode.appendChild(optionNode);
    }
  } else if (field.type === "textarea") {
    inputNode = document.createElement("textarea");
    inputNode.rows = field.rows || 4;
    inputNode.value = field.value || "";
  } else {
    inputNode = document.createElement("input");
    inputNode.type = field.type || "text";
    inputNode.value = field.value || "";
  }

  inputNode.dataset.fieldKey = field.key;
  if (field.placeholder) {
    inputNode.placeholder = field.placeholder;
  }
  labelNode.appendChild(inputNode);
  return labelNode;
}

function updateTemplateVariableEditor(card, channel) {
  const form = card.querySelector(".channel-form");
  const editor = card.querySelector("[data-template-variable-editor]");
  if (!editor || !form) return;

  const values = currentChannelValues(channel.key, card);
  const template = selectedChannelTemplate(channel, values);
  const templateVariables = extractTemplateVariables(template)
    .map((label) => ({
      key: normalizeTemplateVariableKey(label),
      label,
    }))
    .filter((item) => item.key);

  editor.innerHTML = "";

  const heading = document.createElement("div");
  heading.className = "template-variable-header";
  heading.innerHTML = `
    <strong>Template variables</strong>
    <span>${template ? escapeHtml(template.template_key) : "No matching template selected yet"}</span>
  `;
  editor.appendChild(heading);

  if (templateVariables.length === 0) {
    const placeholder = document.createElement("div");
    placeholder.className = "placeholder template-variable-placeholder";
    placeholder.textContent = "No placeholders detected in the selected template.";
    editor.appendChild(placeholder);
  } else {
    const grid = document.createElement("div");
    grid.className = "template-variable-grid";
    for (const variable of templateVariables) {
      const node = buildFieldNode({
        key: variable.key,
        label: variable.label,
        type: "text",
        value: values[variable.key] || values[variable.label] || "",
        placeholder: `Enter ${variable.label}`,
      });
      grid.appendChild(node);
    }
    editor.appendChild(grid);
  }

  const extraField = buildFieldNode({
    key: "template_variables",
    label: "Extra variables JSON",
    type: "textarea",
    rows: 8,
    value: values.template_variables || "{}",
    placeholder: '{ "custom_key": "custom value" }',
  });
  editor.appendChild(extraField);

  const media = templateMediaSpec(template);
  if (media.type && media.type !== "text") {
    const mediaHeading = document.createElement("div");
    mediaHeading.className = "template-variable-header";
    mediaHeading.innerHTML = `
      <strong>Media settings</strong>
      <span>${escapeHtml(media.type)} templates need a public media URL</span>
    `;
    editor.appendChild(mediaHeading);

    const mediaGrid = document.createElement("div");
    mediaGrid.className = "template-variable-grid";
    mediaGrid.appendChild(
      buildFieldNode({
        key: "media_url",
        label: "Media URL",
        type: "url",
        value: values.media_url || media.url || "",
        placeholder: "https://example.com/media.jpg",
      }),
    );
    if (media.type === "document" || media.fileName) {
      mediaGrid.appendChild(
        buildFieldNode({
          key: "media_file_name",
          label: "Media file name",
          type: "text",
          value: values.media_file_name || media.fileName || "",
          placeholder: "policy.pdf",
        }),
      );
    }
    if (media.title) {
      mediaGrid.appendChild(
        buildFieldNode({
          key: "media_title",
          label: "Media title",
          type: "text",
          value: values.media_title || media.title || "",
          placeholder: "Optional media title",
        }),
      );
    }
    editor.appendChild(mediaGrid);
  }
}

function renderChannelStudio() {
  if (!els.channelGrid) return;
  const template = document.querySelector("#channelTemplate");
  const fieldTemplate = document.querySelector("#fieldTemplate");

  els.channelGrid.innerHTML = "";

  for (const channel of state.bootstrap.channels) {
    const card = template.content.firstElementChild.cloneNode(true);
    card.dataset.channel = channel.key;
    const form = card.querySelector(".channel-form");
    const icon = card.querySelector(".channel-icon");
    const label = card.querySelector(".channel-label");
    const route = card.querySelector(".channel-route");
    const binding = card.querySelector("[data-binding-set]");
    const templateKey = card.querySelector("[data-template-key]");
    const connectorURL = card.querySelector("[data-connector-url]");

    icon.textContent = channel.icon;
    icon.style.background = `linear-gradient(135deg, ${channel.accent} 0%, rgba(255,255,255,0.88) 100%)`;
    label.textContent = channel.label;
    route.textContent = channel.route_name;
    binding.textContent = channel.binding_set;
    templateKey.textContent = channel.template_key;
    connectorURL.textContent = channel.connector_url;

    const selectedTemplate = channelTemplates(channel.key).find((item) => item.template_key === channel.template_key) || null;
    const templateChoice = selectedTemplate ? templateChoiceValue(selectedTemplate) : "";
    const storedValues = {
      binding_set: channel.binding_set,
      template_choice: templateChoice,
      template_key: channel.template_key,
      language_code: channel.language_code,
      template_variables: JSON.stringify(defaultTemplateVariables(channel, selectedTemplate), null, 2),
      ...channel.fields.reduce((acc, field) => ({ ...acc, [field.key]: field.value || field.placeholder || "" }), {}),
      ...loadStoredFields(channel.key),
    };

    const sharedFields = [
      {
        key: "template_choice",
        label: "Template",
        type: "select",
        value: templateChoice,
        options: [
          { value: "", label: "Channel default" },
          ...channelTemplates(channel.key).map((item) => ({
            value: templateChoiceValue(item),
            label: templateLabel(item),
          })),
        ],
      },
      { key: "binding_set", label: "Binding set", type: "text", value: channel.binding_set, placeholder: channel.binding_set },
      { key: "template_key", label: "Template key", type: "text", value: channel.template_key, placeholder: channel.template_key },
      { key: "language_code", label: "Language code", type: "text", value: channel.language_code, placeholder: channel.language_code },
    ];

    for (const field of sharedFields) {
      form.appendChild(buildFieldNode({
        ...field,
        value: storedValues[field.key] || "",
      }));
    }

    const templateEditor = document.createElement("div");
    templateEditor.className = "template-variable-editor";
    templateEditor.dataset.templateVariableEditor = "true";
    form.appendChild(templateEditor);

    for (const field of channel.fields) {
      form.appendChild(buildFieldNode({
        ...field,
        value: storedValues[field.key] || "",
      }));
    }

    const handleFieldChange = (event) => {
      const input = event.target.closest("[data-field-key]");
      if (!input) return;

      if (input.dataset.fieldKey === "template_choice") {
        const { templateKey, languageCode } = parseTemplateChoiceValue(input.value);
        const keyInput = card.querySelector('[data-field-key="template_key"]');
        const languageInput = card.querySelector('[data-field-key="language_code"]');
        if (keyInput) keyInput.value = templateKey;
        if (languageInput) languageInput.value = languageCode || channel.language_code || "en";
        updateTemplateVariableEditor(card, channel);
      } else if (input.dataset.fieldKey === "template_key" || input.dataset.fieldKey === "language_code") {
        const templates = channelTemplates(channel.key);
        const matching = templates.find(
          (item) =>
            item.template_key === card.querySelector('[data-field-key="template_key"]')?.value?.trim() &&
            normalizeLanguageCode(item.language_code) === normalizeLanguageCode(card.querySelector('[data-field-key="language_code"]')?.value),
        );
        const choiceInput = card.querySelector('[data-field-key="template_choice"]');
        if (choiceInput && matching) {
          choiceInput.value = templateChoiceValue(matching);
        }
        updateTemplateVariableEditor(card, channel);
      }

      saveStoredFields(channel.key, currentChannelValues(channel.key, card));
      previewPayload(channel, card);
    };

    form.addEventListener("input", handleFieldChange);
    form.addEventListener("change", handleFieldChange);

    updateTemplateVariableEditor(card, channel);
    previewPayload(channel, card);
    setStatus(card, "warn", "Idle");
    els.channelGrid.appendChild(card);
  }
}

function syncChannelTemplateSelectors() {
  if (!els.channelGrid) return;

  for (const card of els.channelGrid.querySelectorAll(".channel-card")) {
    if (card.contains(document.activeElement)) {
      continue;
    }

    const channel = state.channels.get(card.dataset.channel);
    if (!channel) continue;

    const choiceInput = card.querySelector('[data-field-key="template_choice"]');
    if (!choiceInput) continue;

    const values = currentChannelValues(channel.key, card);
    const currentChoice = choiceInput.value;
    const templates = channelTemplates(channel.key);
    const matchingTemplate = selectedChannelTemplate(channel, values);
    const matchingChoice = matchingTemplate ? templateChoiceValue(matchingTemplate) : "";

    choiceInput.innerHTML = [
      `<option value="">Channel default</option>`,
      ...templates.map((item) => {
        const value = templateChoiceValue(item);
        const label = templateLabel(item);
        const selected = value === currentChoice || value === matchingChoice ? " selected" : "";
        return `<option value="${escapeHtml(value)}"${selected}>${escapeHtml(label)}</option>`;
      }),
    ].join("");

    choiceInput.value = templates.some((item) => templateChoiceValue(item) === currentChoice)
      ? currentChoice
      : matchingChoice;

    updateTemplateVariableEditor(card, channel);
    previewPayload(channel, card);
  }
}

function requestTitle(request) {
  return request.request?.template_key || request.request?.event_name || request.request_id;
}

function renderRequestFeed() {
  if (!els.requestFeed) return;
  if (!state.requestOrder.length) {
    els.requestFeed.innerHTML = `<div class="placeholder">Send a channel request to see the timeline appear here.</div>`;
    return;
  }

  const cards = [];
  for (const requestId of state.requestOrder) {
    const entry = state.requests.get(requestId);
    if (!entry) continue;
    const attempts = entry.delivery_attempts || [];
    const latestAttempt = attempts[attempts.length - 1];
    const webhooks = entry.webhook_delivery_attempts || [];
    const webhookSucceeded = webhooks.filter((item) => item.status === "succeeded").length;
    const status = entry.request?.status || entry.status || "accepted";
    const tone = channelStatusTone(status);
    cards.push(`
      <article class="feed-item">
        <div class="feed-item-top">
          <div>
            <strong>${escapeHtml(channelLabel[entry.request?.channels?.[0]] || entry.request?.channels?.[0] || "Notification")}</strong>
            <div class="feed-meta">
              <span>${escapeHtml(requestTitle(entry))}</span>
              <span>•</span>
              <span>${escapeHtml(entry.request_id || requestId)}</span>
            </div>
          </div>
          <span class="status status-${tone}">${escapeHtml(status)}</span>
        </div>
        <div class="feed-body">
          ${escapeHtml(
            latestAttempt
              ? `${latestAttempt.connector_name || "connector"} → ${latestAttempt.status}${latestAttempt.provider_message_id ? ` • ${latestAttempt.provider_message_id}` : ""}`
              : "Awaiting first delivery attempt."
          )}
        </div>
        <div class="feed-meta">
          <span>${escapeHtml(formatTime(entry.request?.updated_at || entry.request?.requested_at || entry.updated_at))}</span>
        </div>
      </article>
    `);
  }

  els.requestFeed.innerHTML = cards.join("") || `<div class="placeholder">No live requests captured yet.</div>`;
}

function renderWebhookFeed() {
  if (!els.webhookFeed) return;
  if (!state.webhookEvents.length) {
    els.webhookFeed.innerHTML = `<div class="placeholder">No webhook receipts yet. Send a notification to populate the inbox.</div>`;
    return;
  }

  els.webhookFeed.innerHTML = state.webhookEvents
    .map(
      (event) => `
      <article class="feed-item">
        <div class="feed-item-top">
          <div>
            <strong>${escapeHtml(event.event_type || "callback")}</strong>
            <div class="feed-meta">
              <span>${escapeHtml(event.request_id || "no request id")}</span>
              <span>•</span>
              <span>${escapeHtml(event.status || "ok")}</span>
            </div>
          </div>
          <span class="status status-${channelStatusTone(event.status || "accepted")}">${escapeHtml(event.status || "ok")}</span>
        </div>
        <div class="feed-body">${escapeHtml(event.summary || event.raw?.slice(0, 240) || "Webhook received.")}</div>
        <div class="feed-meta">
          <span>${escapeHtml(event.source || "control plane")}</span>
          <span>•</span>
          <span>${escapeHtml(formatTime(event.received_at))}</span>
        </div>
      </article>
    `
    )
    .join("");
}

function renderProviderEvents() {
  if (!els.providerEventFeed) return;
  if (!state.providerEvents.length) {
    els.providerEventFeed.innerHTML = `<div class="placeholder">Provider channel events will appear here when inbound callbacks arrive.</div>`;
    return;
  }

  els.providerEventFeed.innerHTML = state.providerEvents
    .map((event) => {
      const detail = [
        event.provider_key,
        event.channel,
        event.event_type,
        event.status,
        event.body || event.media_type || "",
      ]
        .filter(Boolean)
        .join(" • ");

      return `
        <article class="feed-item">
          <div class="feed-item-top">
            <div>
              <strong>${escapeHtml(event.provider_key || "provider event")}</strong>
              <div class="feed-meta">
                <span>${escapeHtml(event.channel || "channel")}</span>
                <span>•</span>
                <span>${escapeHtml(event.direction || "inbound")}</span>
              </div>
            </div>
            <span class="status status-${channelStatusTone(event.status || "accepted")}">${escapeHtml(event.status || "ok")}</span>
          </div>
          <div class="feed-body">${escapeHtml(detail)}</div>
          <div class="feed-meta">
            <span>${escapeHtml(event.external_message_id || event.reply_to_message_id || event.conversation_id || "")}</span>
            <span>•</span>
            <span>${escapeHtml(formatTime(event.received_at || event.created_at))}</span>
          </div>
        </article>
      `;
    })
    .join("");
}

function updateStats() {
  if (!els.stats?.accepted) return;
  const requests = Array.from(state.requests.values());
  const accepted = requests.filter((item) => ["accepted", "processing", "dispatched", "delivered"].includes(item.request?.status || item.status)).length;
  const dispatched = requests.filter((item) => ["dispatched", "delivered"].includes(item.request?.status || item.status)).length;
  const failed = requests.filter((item) => ["failed", "expired", "suppressed", "unsupported"].includes(item.request?.status || item.status)).length;
  const webhooks = state.webhookEvents.length;

  els.stats.accepted.textContent = String(accepted);
  els.stats.dispatched.textContent = String(dispatched);
  els.stats.failed.textContent = String(failed);
  els.stats.webhooks.textContent = String(webhooks);

  document.querySelectorAll('[data-hero-stat="accepted"]').forEach((node) => {
    node.textContent = String(accepted);
  });
  document.querySelectorAll('[data-hero-stat="webhooks"]').forEach((node) => {
    node.textContent = String(webhooks);
  });
  document.querySelectorAll('[data-hero-stat="failed"]').forEach((node) => {
    node.textContent = String(failed);
  });
}

async function refreshRequestDetails() {
  const requestIds = [...new Set(state.requestOrder)].slice(0, 30);
  for (const requestId of requestIds) {
    try {
      const data = await fetchAPIJSON(`/v1/notification-requests/${encodeURIComponent(requestId)}`);
      state.requests.set(requestId, data);
    } catch (error) {
      const existing = state.requests.get(requestId) || {};
      state.requests.set(requestId, {
        ...existing,
        request_id: requestId,
        request: {
          ...(existing.request || {}),
          status: "failed",
          updated_at: new Date().toISOString(),
        },
        delivery_attempts: existing.delivery_attempts || [],
        webhook_delivery_attempts: existing.webhook_delivery_attempts || [],
        error: String(error),
      });
    }
  }
}

async function refreshAll() {
  if (!state.ready) return;
  const editingForm = isEditingForm();
  try {
    await Promise.all([
      loadBootstrap(),
      loadCatalogs(),
      loadWebhookEvents(),
      loadProviderEvents(),
      refreshRequestDetails(),
    ]);
    renderBootstrapProfile();
    if (!editingForm) {
      syncChannelTemplateSelectors();
      updateAdminProviderReferences();
    }
    renderWebhookFeed();
    renderProviderEvents();
    renderRequestFeed();
    updateStats();
    updateReadinessControls();
    if (els.livePill && state.bootstrap?.ready) {
      els.livePill.innerHTML = `<span class="pulse-dot"></span>Live refresh ${new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}`;
    }
  } catch (error) {
    if (els.livePill) {
      els.livePill.textContent = `Disconnected: ${error.message}`;
    }
  }
}

function connectLiveStream() {
  if (state.eventSource) {
    state.eventSource.close();
  }

  const source = new EventSource("/demo/events");
  state.eventSource = source;

  source.onopen = () => {
    els.livePill.innerHTML = `<span class="pulse-dot"></span>Live stream connected`;
  };

  source.onmessage = () => {
    refreshAll().catch((error) => console.error(error));
  };

  source.onerror = () => {
    els.livePill.textContent = "Live stream reconnecting";
  };
}

async function sendChannel(channelKey, card) {
  const channel = state.channels.get(channelKey);
  const formValues = currentChannelValues(channelKey, card);
  saveStoredFields(channelKey, formValues);
  const payload = buildPayload(channel, formValues);
  const preview = card.querySelector(".json-preview");

  if (!state.bootstrap?.ready) {
    console.warn(state.bootstrap?.last_error || "demo is still bootstrapping, sending anyway");
  }

  setStatus(card, "warn", "Sending");
  preview.textContent = JSON.stringify(payload, null, 2);

  try {
    const headers = {
      "Content-Type": "application/json",
    };
    const apiKey = getActiveClientAPIKey();
    if (apiKey) {
      headers.Authorization = `Bearer ${apiKey}`;
    }
    const response = await fetchAPIJSON("/v1/notification-requests", {
      method: "POST",
      body: JSON.stringify(payload),
      headers,
    });

    state.requestOrder = [
      response.request_id,
      ...state.requestOrder.filter((id) => id !== response.request_id),
    ];
    persistRequestOrder();
    setStatus(card, "ok", response.status || "accepted");
    await refreshRequestDetails();
    await loadWebhookEvents();
    renderWebhookFeed();
    renderRequestFeed();
    updateStats();
    return response;
  } catch (error) {
    setStatus(card, "bad", "Failed");
    preview.textContent = `${JSON.stringify(payload, null, 2)}\n\n// ${error.message}`;
    throw error;
  }
}

async function sendSuite() {
  for (const channel of state.bootstrap.channels) {
    const card = els.channelGrid.querySelector(`.channel-card[data-channel="${channel.key}"]`);
    if (!card) continue;
    // eslint-disable-next-line no-await-in-loop
    await sendChannel(channel.key, card);
    // small cadence so the live rail visibly updates between sends
    // eslint-disable-next-line no-await-in-loop
    await new Promise((resolve) => setTimeout(resolve, 800));
  }
}

function bindGlobalActions() {
  if (els.clientSelect) {
    els.clientSelect.addEventListener("change", () => {
      setClientContextByKey(els.clientSelect.value);
      renderBootstrapProfile();
    });
  }

  document.addEventListener("click", async (event) => {
    const button = event.target.closest("[data-action]");
    if (!button) return;
    event.preventDefault();

    try {
      if (button.dataset.action === "refresh-all") {
        await refreshAll();
      } else if (button.dataset.action === "reset-demo-state") {
        clearNotifyHubStorage();
        window.location.reload();
      } else if (button.dataset.action === "refresh-requests") {
        await refreshRequestDetails();
        renderRequestFeed();
        updateStats();
      } else if (button.dataset.action === "refresh-webhooks") {
        await loadWebhookEvents();
        renderWebhookFeed();
        updateStats();
      } else if (button.dataset.action === "refresh-provider-events") {
        await loadProviderEvents();
        renderProviderEvents();
      } else if (button.dataset.action === "send-suite") {
        button.disabled = true;
        try {
          await sendSuite();
        } finally {
          button.disabled = false;
        }
      } else if (button.dataset.action === "prefill") {
        const card = button.closest(".channel-card");
        const channel = state.channels.get(card?.dataset.channel);
        if (card && channel) {
          const selectedTemplate = selectedChannelTemplate(channel, {
            template_key: channel.template_key,
            language_code: channel.language_code,
          });
          const defaults = {
            template_choice: selectedTemplate ? templateChoiceValue(selectedTemplate) : "",
            binding_set: channel.binding_set,
            template_key: channel.template_key,
            language_code: channel.language_code,
            template_variables: JSON.stringify(defaultTemplateVariables(channel, selectedTemplate), null, 2),
          };
          channel.fields.reduce((acc, field) => {
            acc[field.key] = field.value || field.placeholder || "";
            return acc;
          }, defaults);
          card.querySelectorAll("[data-field-key]").forEach((input) => {
            input.value = defaults[input.dataset.fieldKey] || "";
          });
          updateTemplateVariableEditor(card, channel);
          saveStoredFields(channel.key, currentChannelValues(channel.key, card));
          previewPayload(channel, card);
          setStatus(card, "warn", "Reset");
        }
      } else if (button.dataset.action === "send-channel") {
        const card = button.closest(".channel-card");
        const channel = state.channels.get(card?.dataset.channel);
        if (card && channel) {
          await sendChannel(channel.key, card);
        }
      }
    } catch (error) {
      console.error(error);
      alert(error.message);
    }
  });
}

async function init() {
  ensureNotifyHubStorageVersion();
  seedRequestOrderFromStorage();
  bindGlobalActions();
  await loadBootstrap();
  await loadCatalogs();
  if (!state.activeClient) {
    activateClientContext({
      source: "bootstrap",
      client_id: "bootstrap",
      client_name: state.bootstrap.client_name || "notifyhub-demo-studio",
      api_key: "",
    });
  } else {
    renderClientSwitcher();
  }
  renderBootstrapProfile();
  renderChannelStudio();
  renderAdminConsole();
  state.ready = true;
  updateReadinessControls();
  await refreshAll();

  if (PAGE === "admin") {
    const adminSection = document.querySelector("#admin");
    if (adminSection) {
      adminSection.scrollIntoView({ block: "start" });
    }
  } else if (PAGE === "home") {
    connectLiveStream();
  }

  window.setInterval(() => {
    refreshAll().catch((error) => console.error(error));
  }, state.bootstrap.refresh_ms || 3000);
}

init().catch((error) => {
  console.error(error);
  els.livePill.textContent = `Startup failed: ${error.message}`;
});
