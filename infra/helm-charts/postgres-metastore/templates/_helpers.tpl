{{- define "postgres-metastore.name" -}}
{{- default .Chart.Name .Values.nameOverride -}}
{{- end -}}

{{- define "postgres-metastore.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
