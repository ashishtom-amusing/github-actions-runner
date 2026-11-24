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

{{/*
"true" when the champion should be an Argo Rollout (canary) instead of a
Deployment. rollout.enabled: auto = only if Argo Rollouts is installed, so the
same chart works before and after phase 3E. Challengers are never canaried:
they take no user traffic.
*/}}
{{- define "model-service.useRollout" -}}
{{- $mode := toString .Values.rollout.enabled -}}
{{- if and (eq .Values.role "champion") (or (eq $mode "true") (and (eq $mode "auto") (.Capabilities.APIVersions.Has "argoproj.io/v1alpha1/Rollout"))) -}}
true
{{- else -}}
false
{{- end -}}
{{- end -}}

{{- define "model-service.host" -}}
{{- .Values.ingress.hostSuffix | required "ingress.hostSuffix is required" -}}
{{- end -}}
