package main

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	coreid "github.com/NotifyHub-in/NotifyHub/libs/core/id"
)

//go:embed public/index.html public/admin.html public/styles.css public/app.js public/assets/*
var demoAssets embed.FS

type demoServer struct {
	apiBaseURL      *url.URL
	readToken       string
	adminToken      string
	webhookURL      string
	clientStatePath string

	bootstrap    demoBootstrap
	clientAPIKey string

	mu          sync.RWMutex
	webhookLogs []demoWebhookEvent
	streamMu    sync.RWMutex
	subscribers map[chan string]struct{}
	statusMu    sync.RWMutex
	ready       bool
	lastError   string
}

type demoBootstrap struct {
	AppName       string        `json:"app_name"`
	Tagline       string        `json:"tagline"`
	ClientName    string        `json:"client_name"`
	APIBaseURL    string        `json:"api_base_url"`
	WebhookInbox  string        `json:"webhook_inbox"`
	RefreshMs     int           `json:"refresh_ms"`
	Ready         bool          `json:"ready"`
	Bootstrapping bool          `json:"bootstrapping"`
	LastError     string        `json:"last_error,omitempty"`
	Channels      []demoChannel `json:"channels"`
	Defaults      demoDefaults  `json:"defaults"`
	SupportNotes  []string      `json:"support_notes"`
}

type demoDefaults struct {
	UserID    string `json:"user_id"`
	Email     string `json:"email"`
	Phone     string `json:"phone"`
	PushToken string `json:"push_token"`
	Recipient string `json:"recipient"`
	OTP       string `json:"otp"`
	Reference string `json:"reference"`
	Title     string `json:"title"`
	Body      string `json:"body"`
	Subject   string `json:"subject"`
	Message   string `json:"message"`
}

type demoChannel struct {
	Key              string      `json:"key"`
	Label            string      `json:"label"`
	Accent           string      `json:"accent"`
	Icon             string      `json:"icon"`
	RouteName        string      `json:"route_name"`
	BindingSet       string      `json:"binding_set"`
	ConnectorURL     string      `json:"connector_url"`
	TemplateKey      string      `json:"template_key"`
	LanguageCode     string      `json:"language_code"`
	RecipientLabel   string      `json:"recipient_label"`
	RecipientType    string      `json:"recipient_type"`
	DefaultRecipient string      `json:"default_recipient"`
	Fields           []demoField `json:"fields"`
}

type demoField struct {
	Key         string `json:"key"`
	Label       string `json:"label"`
	Type        string `json:"type"`
	Placeholder string `json:"placeholder"`
	Value       string `json:"value"`
	Rows        int    `json:"rows,omitempty"`
}

type demoWebhookEvent struct {
	ReceivedAt time.Time `json:"received_at"`
	RequestID  string    `json:"request_id,omitempty"`
	EventType  string    `json:"event_type,omitempty"`
	Status     string    `json:"status,omitempty"`
	TargetURL  string    `json:"target_url,omitempty"`
	Source     string    `json:"source,omitempty"`
	Summary    string    `json:"summary,omitempty"`
	Raw        string    `json:"raw,omitempty"`
}

type providerAccount struct {
	ProviderAccountID string `json:"provider_account_id"`
	TenantID          string `json:"tenant_id"`
	ProviderKey       string `json:"provider_key"`
	Enabled           bool   `json:"enabled"`
}

type providerAccountsResponse struct {
	ProviderAccounts []providerAccount `json:"provider_accounts"`
}

type demoClientState struct {
	TenantID   string `json:"tenant_id"`
	ClientName string `json:"client_name"`
	APIKey     string `json:"api_key"`
}

func main() {
	port := envOrDefault("NOTIFYHUB_DEMO_PORT", "8788")
	apiBase := envOrDefault("NOTIFYHUB_DEMO_API_BASE_URL", "http://localhost:8080")
	readToken := envOrDefault("NOTIFYHUB_DEMO_READ_TOKEN", "local-read-token")
	adminToken := envOrDefault("NOTIFYHUB_DEMO_ADMIN_TOKEN", "local-admin-token")
	webhookURL := envOrDefault("NOTIFYHUB_DEMO_WEBHOOK_URL", fmt.Sprintf("http://host.docker.internal:%s/hooks/notification-events", port))
	clientStatePath := envOrDefault("NOTIFYHUB_DEMO_CLIENT_STATE", "/tmp/notifyhub-demo-client.json")

	apiBaseURL, err := url.Parse(apiBase)
	if err != nil {
		log.Fatalf("parse api base url: %v", err)
	}

	srv := &demoServer{
		apiBaseURL:      apiBaseURL,
		readToken:       readToken,
		adminToken:      adminToken,
		webhookURL:      webhookURL,
		clientStatePath: clientStatePath,
		subscribers:     make(map[chan string]struct{}),
		bootstrap: demoBootstrap{
			AppName:      "NotifyHub",
			Tagline:      "Notification control plane demo.",
			ClientName:   "notifyhub-demo-studio",
			APIBaseURL:   "/api",
			WebhookInbox: "/demo/webhooks",
			RefreshMs:    3000,
			SupportNotes: []string{
				"All live reads go through the local proxy so browser CORS never gets in the way.",
				"The demo seeds its own routing, templates, bindings, and webhook inbox.",
				"Webhook deliveries are captured locally and reflected back in the status rail.",
			},
			Defaults: demoDefaults{
				UserID:    "notifyhub-demo-user",
				Email:     "demo@example.com",
				Phone:     "+911234567890",
				PushToken: "demo-fcm-token",
				Recipient: "NotifyHub demo recipient",
				OTP:       "112233",
				Reference: "NH-2026-0619",
				Title:     "NotifyHub live demo",
				Body:      "This is a local end-to-end message from NotifyHub.",
				Subject:   "NotifyHub demo update",
				Message:   "Your message has been routed through the control plane successfully.",
			},
		},
	}

	srv.bootstrap.Channels = []demoChannel{
		{
			Key:              "email",
			Label:            "Email",
			Accent:           "#5eead4",
			Icon:             "✉",
			RouteName:        "demo_email_verify",
			BindingSet:       "tenant-a-email",
			ConnectorURL:     "http://connector-email:8091",
			TemplateKey:      "demo_email_verification",
			LanguageCode:     "en",
			RecipientLabel:   "Email recipient",
			RecipientType:    "email",
			DefaultRecipient: srv.bootstrap.Defaults.Email,
			Fields: []demoField{
				{Key: "email", Label: "Email", Type: "email", Placeholder: srv.bootstrap.Defaults.Email, Value: srv.bootstrap.Defaults.Email},
				{Key: "reference_id", Label: "Reference ID", Type: "text", Placeholder: srv.bootstrap.Defaults.Reference, Value: srv.bootstrap.Defaults.Reference},
			},
		},
		{
			Key:              "sms",
			Label:            "SMS",
			Accent:           "#f59e0b",
			Icon:             "⌁",
			RouteName:        "demo_sms_otp",
			BindingSet:       "tenant-a-sms",
			ConnectorURL:     "http://connector-sms:8092",
			TemplateKey:      "demo_sms_otp",
			LanguageCode:     "en",
			RecipientLabel:   "Phone number",
			RecipientType:    "phone",
			DefaultRecipient: srv.bootstrap.Defaults.Phone,
			Fields: []demoField{
				{Key: "phone", Label: "Phone", Type: "tel", Placeholder: srv.bootstrap.Defaults.Phone, Value: srv.bootstrap.Defaults.Phone},
				{Key: "reference_id", Label: "Reference ID", Type: "text", Placeholder: srv.bootstrap.Defaults.Reference, Value: srv.bootstrap.Defaults.Reference},
			},
		},
		{
			Key:              "whatsapp",
			Label:            "WhatsApp",
			Accent:           "#60a5fa",
			Icon:             "◎",
			RouteName:        "demo_whatsapp_otp",
			BindingSet:       "tenant-a-whatsapp",
			ConnectorURL:     "http://connector-whatsapp:8095",
			TemplateKey:      "demo_whatsapp_otp",
			LanguageCode:     "en",
			RecipientLabel:   "Phone number",
			RecipientType:    "phone",
			DefaultRecipient: srv.bootstrap.Defaults.Phone,
			Fields: []demoField{
				{Key: "phone", Label: "Phone", Type: "tel", Placeholder: srv.bootstrap.Defaults.Phone, Value: srv.bootstrap.Defaults.Phone},
				{Key: "reference_id", Label: "Reference ID", Type: "text", Placeholder: srv.bootstrap.Defaults.Reference, Value: srv.bootstrap.Defaults.Reference},
			},
		},
		{
			Key:              "push",
			Label:            "Push",
			Accent:           "#a78bfa",
			Icon:             "↗",
			RouteName:        "demo_push_announcement",
			BindingSet:       "tenant-a-push",
			ConnectorURL:     "http://connector-push:8094",
			TemplateKey:      "demo_push_update",
			LanguageCode:     "en",
			RecipientLabel:   "FCM token",
			RecipientType:    "push_token",
			DefaultRecipient: srv.bootstrap.Defaults.PushToken,
			Fields: []demoField{
				{Key: "push_token", Label: "FCM token", Type: "textarea", Placeholder: srv.bootstrap.Defaults.PushToken, Value: srv.bootstrap.Defaults.PushToken, Rows: 3},
				{Key: "reference_id", Label: "Reference ID", Type: "text", Placeholder: srv.bootstrap.Defaults.Reference, Value: srv.bootstrap.Defaults.Reference},
			},
		},
	}

	go srv.reconcileUntilReady()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /demo/bootstrap", srv.handleBootstrap)
	mux.HandleFunc("GET /demo/webhooks", srv.handleWebhookInbox)
	mux.HandleFunc("GET /demo/events", srv.handleEventStream)
	mux.HandleFunc("POST /demo/seed", srv.handleSeed)
	mux.HandleFunc("POST /hooks/notification-events", srv.handleWebhookReceiver)
	apiProxy := srv.newAPIProxy()
	adminProxy := srv.newAdminProxy()
	mux.Handle("/api/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.TrimPrefix(r.URL.Path, "/api") == "/v1/notification-requests" && !srv.isReady() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "demo is still bootstrapping"})
			return
		}
		apiProxy.ServeHTTP(w, r)
	}))
	mux.Handle("/demo/admin/", adminProxy)
	mux.HandleFunc("GET /styles.css", func(w http.ResponseWriter, _ *http.Request) {
		serveEmbeddedFile(w, "public/styles.css")
	})
	mux.HandleFunc("GET /app.js", func(w http.ResponseWriter, _ *http.Request) {
		serveEmbeddedFile(w, "public/app.js")
	})
	mux.HandleFunc("GET /admin", srv.serveAdmin)
	mux.HandleFunc("GET /admin/", srv.serveAdmin)
	mux.Handle("/assets/", http.StripPrefix("/assets/", http.FileServer(http.FS(assetsFS("public/assets")))))
	mux.HandleFunc("/", srv.serveIndex)

	addr := ":" + port
	log.Printf("NotifyHub demo studio listening on %s", addr)
	if err := http.ListenAndServe(addr, logRequest(mux)); err != nil {
		log.Fatal(err)
	}
}

func (s *demoServer) handleBootstrap(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.bootstrapSnapshot())
}

func (s *demoServer) handleSeed(w http.ResponseWriter, r *http.Request) {
	if err := s.seedDemo(r.Context()); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "seeded"})
}

func (s *demoServer) handleWebhookInbox(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{"events": s.webhookLogs})
}

func (s *demoServer) handleWebhookReceiver(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "read webhook body"})
		return
	}
	_ = r.Body.Close()

	event := demoWebhookEvent{
		ReceivedAt: time.Now().UTC(),
		Raw:        string(body),
		Source:     "notification-control-plane",
	}

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err == nil {
		if v, ok := payload["request_id"].(string); ok {
			event.RequestID = v
		}
		if v, ok := payload["event_type"].(string); ok {
			event.EventType = v
		}
		if v, ok := payload["status"].(string); ok {
			event.Status = v
		}
		if request, ok := payload["request"].(map[string]any); ok {
			if event.RequestID == "" {
				if v, ok := request["request_id"].(string); ok {
					event.RequestID = v
				}
			}
			if event.Status == "" {
				if v, ok := request["status"].(string); ok {
					event.Status = v
				}
			}
		}
	}

	s.mu.Lock()
	s.webhookLogs = append([]demoWebhookEvent{event}, s.webhookLogs...)
	if len(s.webhookLogs) > 60 {
		s.webhookLogs = s.webhookLogs[:60]
	}
	s.mu.Unlock()
	s.broadcastEvent("webhook")

	writeJSON(w, http.StatusOK, map[string]string{"status": "recorded"})
}

func (s *demoServer) handleEventStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	ch := make(chan string, 8)
	s.addSubscriber(ch)
	defer s.removeSubscriber(ch)

	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	_, _ = io.WriteString(w, "data: connected\n\n")
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case msg := <-ch:
			if msg == "" {
				continue
			}
			_, _ = io.WriteString(w, "data: "+msg+"\n\n")
			flusher.Flush()
		case <-ticker.C:
			_, _ = io.WriteString(w, ": ping\n\n")
			flusher.Flush()
		}
	}
}

func (s *demoServer) serveIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	serveEmbeddedFile(w, "public/index.html")
}

func (s *demoServer) serveAdmin(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/admin" && r.URL.Path != "/admin/" {
		http.NotFound(w, r)
		return
	}
	serveEmbeddedFile(w, "public/admin.html")
}

func (s *demoServer) newAPIProxy() http.Handler {
	proxy := httputil.NewSingleHostReverseProxy(s.apiBaseURL)
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.URL.Path = strings.TrimPrefix(req.URL.Path, "/api")
		if req.URL.Path == "" {
			req.URL.Path = "/"
		}
		if req.Method == http.MethodGet || req.Method == http.MethodHead {
			req.Header.Set("X-Notification-Read-Token", s.readToken)
		} else if req.Method == http.MethodPost && req.URL.Path == "/v1/notification-requests" && s.clientAPIKey != "" && req.Header.Get("Authorization") == "" {
			req.Header.Set("Authorization", "Bearer "+s.clientAPIKey)
		}
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, err error) {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
	}
	return proxy
}

func (s *demoServer) newAdminProxy() http.Handler {
	proxy := httputil.NewSingleHostReverseProxy(s.apiBaseURL)
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.URL.Path = strings.TrimPrefix(req.URL.Path, "/demo/admin")
		if req.URL.Path == "" {
			req.URL.Path = "/"
		}
		req.Header.Set("X-Notification-Admin-Token", s.adminToken)
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, err error) {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
	}
	return proxy
}

func (s *demoServer) seedWithRetry() error {
	var lastErr error
	for attempt := 1; attempt <= 12; attempt++ {
		if err := s.seedDemo(context.Background()); err == nil {
			return nil
		} else {
			lastErr = err
			time.Sleep(2 * time.Second)
		}
	}
	return lastErr
}

func (s *demoServer) reconcileUntilReady() {
	backoff := 2 * time.Second
	for {
		ctx := context.Background()
		if err := s.seedDemo(ctx); err != nil {
			s.setLastError(err.Error())
			time.Sleep(backoff)
			if backoff < 10*time.Second {
				backoff += time.Second
			}
			continue
		}
		if err := s.ensureDemoClient(ctx); err != nil {
			s.setLastError(err.Error())
			time.Sleep(backoff)
			if backoff < 10*time.Second {
				backoff += time.Second
			}
			continue
		}
		s.setReady("")
		return
	}
}

func (s *demoServer) ensureDemoClient(ctx context.Context) error {
	if s.clientAPIKey != "" {
		return nil
	}

	if raw, err := os.ReadFile(s.clientStatePath); err == nil {
		var cached demoClientState
		if err := json.Unmarshal(raw, &cached); err == nil && cached.APIKey != "" {
			s.clientAPIKey = cached.APIKey
			if cached.ClientName != "" {
				s.bootstrap.ClientName = cached.ClientName
			}
			s.setReady("")
			return nil
		}
	}

	state := demoClientState{
		TenantID:   "notifyhub-demo",
		ClientName: "notifyhub-demo-studio-" + coreid.New(4),
	}

	var resp struct {
		Client notificationClient `json:"client"`
		APIKey string             `json:"api_key"`
	}
	if _, err := s.postJSON(ctx, "/v1/clients", map[string]any{
		"tenant_id":        state.TenantID,
		"client_name":      state.ClientName,
		"enabled":          true,
		"allowed_channels": []string{"email", "sms", "whatsapp", "push"},
	}, &resp); err != nil {
		return err
	}

	state.APIKey = resp.APIKey
	if state.APIKey == "" {
		return fmt.Errorf("notification client api key missing")
	}
	s.clientAPIKey = state.APIKey
	s.bootstrap.ClientName = state.ClientName

	encoded, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(s.clientStatePath, encoded, 0o600); err != nil {
		return err
	}
	s.setReady("")
	return nil
}

type notificationClient struct {
	ClientID   string `json:"client_id"`
	TenantID   string `json:"tenant_id"`
	ClientName string `json:"client_name"`
	Enabled    bool   `json:"enabled"`
}

func (s *demoServer) addSubscriber(ch chan string) {
	s.streamMu.Lock()
	s.subscribers[ch] = struct{}{}
	s.streamMu.Unlock()
}

func (s *demoServer) removeSubscriber(ch chan string) {
	s.streamMu.Lock()
	delete(s.subscribers, ch)
	close(ch)
	s.streamMu.Unlock()
}

func (s *demoServer) broadcastEvent(msg string) {
	s.streamMu.RLock()
	defer s.streamMu.RUnlock()
	for ch := range s.subscribers {
		select {
		case ch <- msg:
		default:
		}
	}
}

func (s *demoServer) seedDemo(ctx context.Context) error {
	var accounts providerAccountsResponse
	if err := s.getJSON(ctx, "/v1/provider-accounts", &accounts); err != nil {
		return fmt.Errorf("list provider accounts: %w", err)
	}

	accountID := func(tenantID, providerKey string) (string, error) {
		var fallback string
		for _, acc := range accounts.ProviderAccounts {
			if acc.TenantID == tenantID && acc.ProviderKey == providerKey {
				return acc.ProviderAccountID, nil
			}
			if acc.ProviderKey == providerKey && acc.Enabled && fallback == "" {
				fallback = acc.ProviderAccountID
			}
		}
		if fallback != "" {
			return fallback, nil
		}
		return "", fmt.Errorf("provider account not found for tenant=%s provider=%s", tenantID, providerKey)
	}

	templates := []map[string]any{
		{
			"template_key":     "demo_email_verification",
			"channel":          "email",
			"language_code":    "en",
			"subject_template": "Please verify your e-mail address",
			"body_template":    "Please click on the link to verify your e-mail address. {{verifyEmailUrl}}",
			"enabled":          true,
		},
		{
			"template_key":  "demo_sms_otp",
			"channel":       "sms",
			"language_code": "en",
			"body_template": "{{OTP}} is your verification code {{sms_hash}}",
			"enabled":       true,
		},
		{
			"template_key":  "demo_whatsapp_otp",
			"channel":       "whatsapp",
			"language_code": "en",
			"body_template": "{{otp}} is your verification code.",
			"metadata": map[string]any{
				"editorType":             "normal",
				"media_type":             "text",
				"gupshup_template_name":  "demo_whatsapp_otp",
				"interactive_attributes": "{\"footer\":\"this code expires in 10 minute.\"}",
			},
			"enabled": true,
		},
		{
			"template_key":     "demo_push_update",
			"channel":          "push",
			"language_code":    "en",
			"subject_template": "NotifyHub demo update",
			"body_template":    "Your notification was routed through NotifyHub.",
			"metadata": map[string]any{
				"editorType": "normal",
				"media_type": "text",
			},
			"enabled": true,
		},
	}
	for _, template := range templates {
		if _, err := s.postJSON(ctx, "/v1/templates", template, nil); err != nil {
			return fmt.Errorf("upsert template %s: %w", template["template_key"], err)
		}
	}

	emailID, err := accountID("tenant-a", "smtp-email")
	if err != nil {
		return err
	}
	smsID, err := accountID("tenant-a", "gupshup-sms")
	if err != nil {
		return err
	}
	whatsappID, err := accountID("tenant-a", "gupshup-whatsapp")
	if err != nil {
		return err
	}
	pushID, err := accountID("tenant-a", "fcm-push")
	if err != nil {
		return err
	}

	bindings := []map[string]any{
		{
			"channel":             "email",
			"binding_set":         "tenant-a-email",
			"connector_name":      "connector-email",
			"endpoint_url":        "http://connector-email:8091",
			"provider_account_id": emailID,
			"enabled":             true,
			"priority":            10,
		},
		{
			"channel":             "sms",
			"binding_set":         "tenant-a-sms",
			"connector_name":      "connector-sms",
			"endpoint_url":        "http://connector-sms:8092",
			"provider_account_id": smsID,
			"enabled":             true,
			"priority":            10,
		},
		{
			"channel":             "whatsapp",
			"binding_set":         "tenant-a-whatsapp",
			"connector_name":      "connector-whatsapp",
			"endpoint_url":        "http://connector-whatsapp:8095",
			"provider_account_id": whatsappID,
			"enabled":             true,
			"priority":            10,
		},
		{
			"channel":             "push",
			"binding_set":         "tenant-a-push",
			"connector_name":      "connector-push",
			"endpoint_url":        "http://connector-push:8094",
			"provider_account_id": pushID,
			"enabled":             true,
			"priority":            10,
		},
	}
	for _, binding := range bindings {
		if _, err := s.postJSON(ctx, "/v1/provider-bindings", binding, nil); err != nil {
			return fmt.Errorf("upsert provider binding %s: %w", binding["binding_set"], err)
		}
	}

	routes := []map[string]any{
		{"event_name": "demo_email_verify", "channels": []string{"email"}, "binding_set": "tenant-a-email", "enabled": true, "priority": 10},
		{"event_name": "demo_sms_otp", "channels": []string{"sms"}, "binding_set": "tenant-a-sms", "enabled": true, "priority": 10},
		{"event_name": "demo_whatsapp_otp", "channels": []string{"whatsapp"}, "binding_set": "tenant-a-whatsapp", "enabled": true, "priority": 10},
		{"event_name": "demo_push_announcement", "channels": []string{"push"}, "binding_set": "tenant-a-push", "enabled": true, "priority": 10},
	}
	for _, route := range routes {
		if _, err := s.postJSON(ctx, "/v1/routing-policies", route, nil); err != nil {
			return fmt.Errorf("upsert routing policy %s: %w", route["event_name"], err)
		}
	}

	for _, channel := range []string{"email", "sms", "whatsapp", "push"} {
		policy := map[string]any{
			"user_id":    s.bootstrap.Defaults.UserID,
			"channel":    channel,
			"is_enabled": true,
		}
		if _, err := s.postJSON(ctx, "/v1/preference-policies", policy, nil); err != nil {
			return fmt.Errorf("upsert preference policy %s: %w", channel, err)
		}
	}

	if _, err := s.postJSON(ctx, "/v1/webhook-subscriptions", map[string]any{
		"target_url": s.webhookURL,
		"enabled":    true,
	}, nil); err != nil {
		return fmt.Errorf("upsert webhook subscription: %w", err)
	}

	return nil
}

func (s *demoServer) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.apiBaseURL.ResolveReference(&url.URL{Path: path}).String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Notification-Read-Token", s.readToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (s *demoServer) postJSON(ctx context.Context, path string, in any, out any) (*http.Response, error) {
	body, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.apiBaseURL.ResolveReference(&url.URL{Path: path}).String(), strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Notification-Admin-Token", s.adminToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		return resp, fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	if out != nil {
		defer resp.Body.Close()
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return resp, err
		}
		return resp, nil
	}

	_ = resp.Body.Close()
	return resp, nil
}

func (s *demoServer) resolveProviderAccountID(tenantID, providerKey string) (string, error) {
	var accounts providerAccountsResponse
	if err := s.getJSON(context.Background(), "/v1/provider-accounts", &accounts); err != nil {
		return "", err
	}
	var fallback string
	for _, acc := range accounts.ProviderAccounts {
		if acc.TenantID == tenantID && acc.ProviderKey == providerKey {
			return acc.ProviderAccountID, nil
		}
		if acc.ProviderKey == providerKey && acc.Enabled && fallback == "" {
			fallback = acc.ProviderAccountID
		}
	}
	if fallback != "" {
		return fallback, nil
	}
	return "", fmt.Errorf("provider account not found for tenant=%s provider=%s", tenantID, providerKey)
}

func (s *demoServer) bootstrapSnapshot() demoBootstrap {
	bootstrap := s.bootstrap
	bootstrap.Ready = s.isReady()
	bootstrap.Bootstrapping = !bootstrap.Ready
	bootstrap.LastError = s.currentError()
	return bootstrap
}

func (s *demoServer) setReady(errMsg string) {
	s.statusMu.Lock()
	defer s.statusMu.Unlock()
	s.ready = true
	s.lastError = errMsg
}

func (s *demoServer) setLastError(errMsg string) {
	s.statusMu.Lock()
	defer s.statusMu.Unlock()
	s.ready = false
	s.lastError = errMsg
}

func (s *demoServer) isReady() bool {
	s.statusMu.RLock()
	defer s.statusMu.RUnlock()
	return s.ready
}

func (s *demoServer) currentError() string {
	s.statusMu.RLock()
	defer s.statusMu.RUnlock()
	return s.lastError
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func assetsFS(dir string) fs.FS {
	sub, err := fs.Sub(demoAssets, dir)
	if err != nil {
		panic(err)
	}
	return sub
}

func serveEmbeddedFile(w http.ResponseWriter, name string) {
	content, err := demoAssets.ReadFile(name)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Cache-Control", "no-store, max-age=0")
	switch {
	case strings.HasSuffix(name, ".html"):
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
	case strings.HasSuffix(name, ".css"):
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
	case strings.HasSuffix(name, ".js"):
		w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	case strings.HasSuffix(name, ".png"):
		w.Header().Set("Content-Type", "image/png")
	}
	_, _ = w.Write(content)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func logRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
	})
}
