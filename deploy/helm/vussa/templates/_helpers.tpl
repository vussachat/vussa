{{- define "vussa.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- define "vussa.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "vussa.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
