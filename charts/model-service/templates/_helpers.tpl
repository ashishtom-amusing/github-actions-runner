{{- define "model-service.model" -}}
{{- .Values.modelName | required "modelName is required" -}}
{{- end -}}

{{/*
Resource name. The champion keeps the bare model name so its Deployment
selector never changes (selectors are immutable). Other roles get a suffix.
*/}}
{{- define "model-service.fullname" -}}
{{- if eq .Values.role "champion" -}}
{{- include "model-service.model" . -}}
{{- else -}}
{{- printf "%s-%s" (include "model-service.model" .) .Values.role -}}
{{- end -}}
{{- end -}}

{{- define "model-service.labels" -}}
app.kubernetes.io/name: {{ include "model-service.fullname" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Values.modelVersion | quote }}
mlops/model: {{ include "model-service.model" . }}
mlops/role: {{ .Values.role }}
{{- end -}}

{{- define "model-service.selectorLabels" -}}
app.kubernetes.io/name: {{ include "model-service.fullname" . }}
{{- end -}}
