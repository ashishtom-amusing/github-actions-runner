{{- define "model-service.name" -}}
{{- .Values.modelName | required "modelName is required" -}}
{{- end -}}

{{- define "model-service.labels" -}}
app.kubernetes.io/name: {{ include "model-service.name" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Values.modelVersion | quote }}
mlops/model: {{ include "model-service.name" . }}
{{- end -}}

{{- define "model-service.selectorLabels" -}}
app.kubernetes.io/name: {{ include "model-service.name" . }}
{{- end -}}
