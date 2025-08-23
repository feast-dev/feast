{{/*
Expand the name of the chart.
*/}}
{{- define "feast-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Generate a DNS-1123 compatible name with a suffix, ensuring total length <= 63
Usage: {{ include "feast-operator.fullnameWithSuffix" (dict "root" . "suffix" "metrics") }}
*/}}
{{- define "feast-operator.fullnameWithSuffix" -}}
{{- $root := .root -}}
{{- $suffix := .suffix -}}
{{- $base := include "feast-operator.fullname" $root -}}
{{- $max := 63 -}}
{{- $needed := add (len $suffix) 1 -}}
{{- $trim := sub $max $needed -}}
{{- printf "%s-%s" (trunc (int $trim) $base) $suffix | trimSuffix "-" -}}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "feast-operator.fullname" -}}
{{- $name := "" }}
{{- if .Values.fullnameOverride }}
{{- $name = .Values.fullnameOverride }}
{{- else }}
{{- $chartName := default .Chart.Name .Values.nameOverride }}
{{- if contains $chartName .Release.Name }}
{{- $name = .Release.Name }}
{{- else }}
{{- $name = printf "%s-%s" .Release.Name $chartName }}
{{- end }}
{{- end }}
{{- $prefix := .Values.namePrefix | default "" }}
{{- printf "%s%s" $prefix $name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "feast-operator.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "feast-operator.labels" -}}
helm.sh/chart: {{ include "feast-operator.chart" . }}
{{ include "feast-operator.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/name: feast-operator
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "feast-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "feast-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
control-plane: controller-manager
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "feast-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (printf "%s-controller-manager" (include "feast-operator.fullname" .)) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the namespace
*/}}
{{- define "feast-operator.namespace" -}}
{{- if .Values.namespace.name }}
{{- .Values.namespace.name }}
{{- else }}
{{- .Release.Namespace }}
{{- end }}
{{- end }}

{{/*
Create image reference
*/}}
{{- define "feast-operator.image" -}}
{{- if .Values.global.imageRegistry }}
{{- printf "%s/%s:%s" .Values.global.imageRegistry .Values.operator.image.repository .Values.operator.image.tag }}
{{- else }}
{{- printf "%s:%s" .Values.operator.image.repository .Values.operator.image.tag }}
{{- end }}
{{- end }}

{{/*
Common annotations
*/}}
{{- define "feast-operator.annotations" -}}
{{- with .Values.commonAnnotations }}
{{ toYaml . }}
{{- end }}
{{- end }}
