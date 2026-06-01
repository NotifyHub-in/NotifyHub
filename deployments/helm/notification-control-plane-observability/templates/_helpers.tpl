{{- define "notification-control-plane-observability.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "notification-control-plane-observability.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-obs" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "notification-control-plane-observability.labels" -}}
app.kubernetes.io/name: {{ include "notification-control-plane-observability.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: notification-control-plane
{{- with .Values.global.labels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{- define "notification-control-plane-observability.selectorLabels" -}}
app.kubernetes.io/name: {{ include "notification-control-plane-observability.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
