{{- define "notification-control-plane.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "notification-control-plane.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s" (include "notification-control-plane.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "notification-control-plane.labels" -}}
app.kubernetes.io/name: {{ include "notification-control-plane.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ default .Chart.AppVersion .Values.global.appVersion | quote }}
app.kubernetes.io/part-of: notification-control-plane
{{- end -}}

{{- define "notification-control-plane.selectorLabels" -}}
app.kubernetes.io/name: {{ include "notification-control-plane.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "notification-control-plane.componentName" -}}
{{- $ctx := index . 0 -}}
{{- $component := index . 1 -}}
{{- printf "%s-%s" (include "notification-control-plane.fullname" $ctx) $component -}}
{{- end -}}

{{- define "notification-control-plane.image" -}}
{{- $ctx := index . 0 -}}
{{- $component := index . 1 -}}
{{- $img := index $ctx.Values.images $component -}}
{{- $tag := default $ctx.Chart.AppVersion $img.tag -}}
{{- printf "%s:%s" $img.repository $tag -}}
{{- end -}}

{{- define "notification-control-plane.authSecretName" -}}
{{- if .Values.auth.existingSecret -}}
{{- .Values.auth.existingSecret -}}
{{- else -}}
{{- printf "%s-auth" (include "notification-control-plane.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "notification-control-plane.secretVolumeName" -}}
{{- if .Values.global.secretVolume.name -}}
{{- .Values.global.secretVolume.name -}}
{{- else -}}
{{- printf "%s-secrets" (include "notification-control-plane.fullname" .) -}}
{{- end -}}
{{- end -}}
