{{/*
Pod template shared by the Deployment (plain mode) and the Rollout (canary mode).
*/}}
{{- define "model-service.podTemplate" -}}
metadata:
  labels: {{- include "model-service.labels" . | nindent 4 }}
  annotations:
    # Forces a rollout when the model version changes even if the tag repeats
    mlops/model-version: {{ .Values.modelVersion | quote }}
spec:
  terminationGracePeriodSeconds: {{ .Values.terminationGracePeriodSeconds }}
  {{- with .Values.imagePullSecrets }}
  imagePullSecrets: {{- toYaml . | nindent 4 }}
  {{- end }}
  containers:
    - name: service
      image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
      imagePullPolicy: {{ .Values.image.pullPolicy }}
      ports:
        - name: http
          containerPort: 8000
      env:
        - name: MODEL_NAME
          value: {{ include "model-service.model" . | quote }}
        - name: MODEL_VERSION
          value: {{ .Values.modelVersion | quote }}
        - name: ROLE
          value: {{ .Values.role | quote }}
        - name: POD_NAME
          valueFrom: { fieldRef: { fieldPath: metadata.name } }
        - name: POD_NAMESPACE
          valueFrom: { fieldRef: { fieldPath: metadata.namespace } }
        {{- if .Values.predictionLog.enabled }}
        - name: PREDICTION_LOG_BUCKET
          value: {{ .Values.predictionLog.bucket | quote }}
        - name: PREDICTION_LOG_BATCH_SIZE
          value: {{ .Values.predictionLog.batchSize | quote }}
        - name: PREDICTION_LOG_FLUSH_SECONDS
          value: {{ .Values.predictionLog.flushSeconds | quote }}
        - name: S3_ENDPOINT_URL
          value: {{ .Values.predictionLog.s3Endpoint | quote }}
        {{- end }}
        - name: SHADOW_ENABLED
          value: {{ and .Values.shadow.enabled (eq .Values.role "champion") | quote }}
        - name: SHADOW_TIMEOUT_SECONDS
          value: {{ .Values.shadow.timeoutSeconds | quote }}
        - name: SHADOW_MAX_INFLIGHT
          value: {{ .Values.shadow.maxInflight | quote }}
        {{- if eq .Values.chaos "errors" }}
        - name: CHAOS_ERROR_RATE
          value: "0.5"
        {{- else if eq .Values.chaos "slow" }}
        - name: CHAOS_LATENCY_SECONDS
          value: "0.8"
        {{- end }}
      {{- if .Values.predictionLog.enabled }}
      envFrom:
        # optional: a missing secret disables logging instead of blocking the pod
        - secretRef:
            name: {{ .Values.predictionLog.s3SecretName }}
            optional: true
      {{- end }}
      startupProbe:
        httpGet: { path: /health, port: http }
        periodSeconds: 5
        failureThreshold: 24        # up to 2 min to load the model
      readinessProbe:
        httpGet: { path: /health, port: http }
        periodSeconds: 10
      livenessProbe:
        httpGet: { path: /health, port: http }
        periodSeconds: 30
      resources: {{- toYaml .Values.resources | nindent 8 }}
{{- end -}}
