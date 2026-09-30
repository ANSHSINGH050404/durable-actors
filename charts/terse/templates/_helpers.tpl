{{- define "terse.name" -}}
{{- printf "%s-terse" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- define "terse.image" -}}
{{- printf "%s@sha256:%s" .Values.image.repository (required "image.digest must be the published runtime sha256 digest" .Values.image.digest) -}}
{{- end -}}
{{- define "terse.validate" -}}
{{- if and .Values.gateway.enabled (eq (empty .Values.gateway.tlsSecret) (empty .Values.gateway.preSharedCert)) }}{{ fail "gateway requires exactly one of tlsSecret or preSharedCert" }}{{ end -}}
{{- if eq .Values.sandboxNamespace .Release.Namespace }}{{ fail "sandboxNamespace must differ from the control-plane namespace" }}{{ end -}}
{{- if not (hasKey .Values.zones .Values.region) }}{{ fail "region must have a configured placement zone" }}{{ end -}}
{{- if lt (int .Values.replicaCount) 2 }}{{ fail "regional availability requires multiple control-plane replicas" }}{{ end -}}
{{- range $region, $placements := .Values.zones -}}
{{- if kindIs "string" $placements }}{{ fail "regional availability requires multiple compute zones" }}{{ end -}}
{{- if lt (len $placements) 2 }}{{ fail "regional availability requires multiple compute zones" }}{{ end -}}
{{- end -}}
{{- $zones := dict -}}{{- $buckets := dict -}}
{{- range .Values.storage.rapid.buckets -}}
{{- if hasKey $zones .zone }}{{ fail "Rapid buckets require distinct zones" }}{{ end -}}
{{- if or (hasKey $buckets .bucket) (eq .bucket $.Values.storage.archiveBucket) }}{{ fail "Rapid and Standard buckets must be distinct" }}{{ end -}}
{{- $_ := set $zones .zone true -}}{{- $_ := set $buckets .bucket true -}}
{{- end -}}
{{- if gt (int .Values.storage.rapid.ackZones) (len $zones) }}{{ fail "ackZones exceeds configured Rapid zones" }}{{ end -}}
{{- end -}}
